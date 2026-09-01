//// A strict superset of `gleam/otp/actor`'s builder, on a receive loop weft
//// owns.
////
//// ## Why a loop of our own
////
//// `gleam/otp/actor` is the right shape and this module does not try to
//// improve on it: `new`, `on_message`, `start`, `continue`, `stop` mean
//// exactly what they mean upstream, `Started`, `StartError` and
//// `ChildSpecification` are upstream's own types rather than copies, and an
//// actor built here drops into a `gleam_otp` supervisor unchanged.
////
//// What upstream cannot give us is a way in. `Builder`, `Initialised` and
//// `Next` are opaque, so there is no wrapping trick that adds a field to
//// them: a `handle_continue` analog has to be able to put a message
//// *somewhere the loop looks before the mailbox*, and only the loop can
//// own that place. So weft reimplements the loop and keeps the surface
//// identical, which is the trade that makes migration a change of import
//// line.
////
//// ## The continue queue, and what its order guarantees
////
//// gen_server's `{continue, Term}` closes a race that every actor with
//// expensive initialisation hits. Sending yourself a message from the
//// initialiser looks equivalent and is not: `start` returns, the parent
//// hands the subject to a client, and the client's first request can be
//// ahead of your own message in the mailbox. The actor then serves a query
//// against half-built state, and grows a "not ready yet" case arm that
//// exists only to paper over the timing.
////
//// `continuing` puts the message in a queue the loop drains *before* it
//// looks at the mailbox at all, and the queue is populated before the
//// acknowledgement that unblocks `start`. There is no interleaving in
//// which an external message wins, because external messages are not
//// consulted while the queue is non-empty.
////
//// The ordering contract, which is observable and which callers will
//// depend on:
////
//// - Injected messages run ahead of everything in the mailbox.
//// - Within one `Next` or one `Initialised`, they run in the order given:
////   `continue(s) |> then_handle(A) |> then_handle(B)` handles `A`, then
////   `B`.
//// - A message injected while handling a queued one runs *before* the rest
////   of the queue. The whole block goes to the front, depth-first, which is
////   what gen_statem's `next_event` does and what makes `then_handle`
////   usable as a small state machine.
//// - System messages are not starved by any of it. The loop checks the
////   debug plane between every two injected messages, so a `sys:suspend/1`
////   arriving in the middle of a continue chain takes effect there rather
////   than after the chain drains.
////
//// ## Example
////
//// ```gleam
//// pub type Message {
////   LoadIndex
////   Query(term: String, reply: process.Subject(List(String)))
//// }
////
//// pub fn start_search() {
////   actor.new_with_initialiser(1000, fn(subject) {
////     // Returns immediately: the supervisor is unblocked and the actor is
////     // registered and reachable. The slow index load still happens before
////     // the first Query is served, with no race.
////     actor.initialised(Empty)
////     |> actor.returning(subject)
////     |> actor.continuing(LoadIndex)
////     |> Ok
////   })
////   |> actor.on_message(handle)
////   |> actor.start
//// }
////
//// fn handle(state: Index, message: Message) -> actor.Next(Index, Message) {
////   case message {
////     LoadIndex -> actor.continue(Loaded(read_index_from_disk()))
////     Query(term:, reply:) -> {
////       process.send(reply, search(state, term))
////       actor.continue(state)
////     }
////   }
//// }
//// ```

import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{
  type ExitReason, type Pid, type Selector, type Subject, Abnormal, Killed,
  Normal,
}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor as otp_actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import weft/internal/sys

// ---------------------------------------------------------------- interop

/// What the parent is given when an actor starts: the actor's pid and
/// whatever its initialiser chose to return.
///
/// This is `gleam/otp/actor`'s own type, not a copy of it, so a weft actor
/// can be started by anything that expects a gleam_otp one.
pub type Started(data) =
  otp_actor.Started(data)

/// The result of starting an actor.
///
/// `gleam/otp/actor`'s own alias, for the same interoperability reason as
/// `Started`.
pub type StartResult(data) =
  otp_actor.StartResult(data)

/// Why an actor failed to start: the initialiser timed out, returned an
/// error, or the process died while running it.
///
/// `gleam/otp/actor`'s own type, so a supervisor cannot tell a weft actor
/// from a gleam_otp one.
pub type StartError =
  otp_actor.StartError

// ------------------------------------------------------------------- next

/// What an actor does after handling a message.
///
/// Built with `continue`, `stop` or `stop_abnormal`, and refined with
/// `with_selector` and `then_handle`.
pub opaque type Next(state, message) {
  /// Carry on with `state`, optionally replacing the selector, and handle
  /// `injected` — in order — before anything in the mailbox.
  Continue(
    state: state,
    selector: Option(Selector(message)),
    injected: List(message),
  )

  /// Stop, exiting with this reason. Any queued injected messages and
  /// anything in the mailbox are discarded.
  Stop(reason: ExitReason)
}

/// Continue, processing any waiting or future messages.
///
/// ## Examples
///
/// ```gleam
/// fn handle(count: Int, _message: Tick) -> actor.Next(Int, Tick) {
///   actor.continue(count + 1)
/// }
/// ```
pub fn continue(state: state) -> Next(state, message) {
  Continue(state:, selector: None, injected: [])
}

/// Stop and shut down, handling no further messages.
///
/// The exit reason is `Normal`, so a `Permanent` child restarts and a
/// `Transient` one does not. An `on_shutdown` callback runs first, with the
/// state as it stood.
///
/// ## Examples
///
/// ```gleam
/// fn handle(state: State, message: Message) -> actor.Next(State, Message) {
///   case message {
///     Shutdown -> actor.stop()
///     Work -> actor.continue(state)
///   }
/// }
/// ```
pub fn stop() -> Next(state, message) {
  Stop(Normal)
}

/// Stop and shut down abnormally, propagating the reason to linked
/// processes.
///
/// An `on_shutdown` callback runs first, with the state as it stood, which
/// is the one chance to release anything the actor still owns before the
/// exit signal goes out.
///
/// ## Examples
///
/// ```gleam
/// fn handle(state: State, message: Message) -> actor.Next(State, Message) {
///   case message {
///     Corrupted -> actor.stop_abnormal("index checksum mismatch")
///     Work -> actor.continue(state)
///   }
/// }
/// ```
pub fn stop_abnormal(reason: String) -> Next(state, message) {
  Stop(Abnormal(dynamic.string(reason)))
}

/// Replace the selector the actor receives with going forward.
///
/// This replaces any selector given by the initialiser or by an earlier
/// `Next`, rather than adding to it, so a selector that no longer selects
/// the actor's own subject stops receiving on it.
///
/// Applying this to a `stop` value does nothing: there is no "going
/// forward".
///
/// ## Examples
///
/// ```gleam
/// actor.continue(state)
/// |> actor.with_selector(process.new_selector() |> process.select(subject))
/// ```
pub fn with_selector(
  value: Next(state, message),
  selector: Selector(message),
) -> Next(state, message) {
  case value {
    Continue(state:, selector: _, injected:) ->
      Continue(state:, selector: Some(selector), injected:)
    Stop(reason:) -> Stop(reason:)
  }
}

/// Handle `message` next, ahead of the mailbox.
///
/// This is `continuing` from a message handler, and the second half of the
/// `handle_continue` analog: a handler can schedule follow-up work that is
/// guaranteed to run before any client request, without the "not ready yet"
/// arm a self-send would need.
///
/// Ordering is the contract in this module's header, and it is worth
/// repeating because it is what callers depend on. Messages injected by one
/// handler run in the order they were added, and the whole block runs before
/// anything queued earlier — depth-first, as gen_statem's `next_event` is.
/// A handler that injects on every call therefore never lets the queue
/// drain, and never reads its mailbox again; that is a live-lock the type
/// system cannot catch.
///
/// Applying this to a `stop` value does nothing: an actor that is stopping
/// handles no more messages.
///
/// ## Examples
///
/// ```gleam
/// // `Reindex` is handled before any client request already in the mailbox.
/// actor.continue(state) |> actor.then_handle(Reindex)
/// ```
///
/// ```gleam
/// // `Flush` is handled first, then `Compact`, then the mailbox.
/// actor.continue(state)
/// |> actor.then_handle(Flush)
/// |> actor.then_handle(Compact)
/// ```
pub fn then_handle(
  next: Next(state, message),
  message: message,
) -> Next(state, message) {
  case next {
    // Appending keeps the call-site order readable at the cost of a walk
    // over a list that is, by construction, the handful of messages one
    // handler chose to inject.
    Continue(state:, selector:, injected:) ->
      Continue(state:, selector:, injected: list.append(injected, [message]))
    Stop(reason:) -> Stop(reason:)
  }
}

// ------------------------------------------------------------ initialised

/// The outcome of an actor's initialiser: the starting state, the selector
/// to receive with, the value to hand back to the parent, and any messages
/// to handle before the mailbox.
///
/// Built with `initialised` and refined with `selecting`, `returning` and
/// `continuing`.
pub opaque type Initialised(state, message, data) {
  Initialised(
    state: state,
    selector: Option(Selector(message)),
    return: data,
    injected: List(message),
  )
}

/// Take the post-initialisation state of the actor.
///
/// ## Examples
///
/// ```gleam
/// actor.new_with_initialiser(1000, fn(subject) {
///   actor.initialised(0) |> actor.returning(subject) |> Ok
/// })
/// ```
pub fn initialised(state: state) -> Initialised(state, message, Nil) {
  Initialised(state:, selector: None, return: Nil, injected: [])
}

/// Give the actor a selector to receive messages with.
///
/// This replaces the default selector, which selects only the actor's own
/// subject, so a custom selector must select that subject itself if the
/// actor is still to receive on it. A message that arrives and is not
/// selected for is discarded with a warning.
///
/// ## Deviation from `gleam/otp/actor`
///
/// Upstream's `selecting` may change the actor's message type, because
/// upstream's `Initialised` mentions that type only in the selector field.
/// Here the injected-message queue mentions it too, so changing the type
/// would mean silently discarding anything `continuing` had already added.
/// The signature therefore fixes the message type instead. This rejects no
/// program upstream accepts: before `selecting`, an upstream `Initialised`
/// always has an as-yet-unbound message type, and unifying it with the
/// selector's is exactly what the type-changing version does.
///
/// ## Examples
///
/// ```gleam
/// actor.new_with_initialiser(1000, fn(subject) {
///   let selector =
///     process.new_selector()
///     |> process.select(subject)
///     |> process.select_map(pubsub_subject, Broadcast)
///   actor.initialised(state)
///   |> actor.selecting(selector)
///   |> actor.returning(subject)
///   |> Ok
/// })
/// ```
pub fn selecting(
  initialised: Initialised(state, message, return),
  selector: Selector(message),
) -> Initialised(state, message, return) {
  Initialised(..initialised, selector: Some(selector))
}

/// Set the value handed back to the parent when the actor has started.
///
/// Commonly the subject the actor receives on, so the parent can send to it.
///
/// ## Examples
///
/// ```gleam
/// actor.new_with_initialiser(1000, fn(subject) {
///   actor.initialised(state) |> actor.returning(subject) |> Ok
/// })
/// ```
pub fn returning(
  initialised: Initialised(state, message, old_return),
  return: return,
) -> Initialised(state, message, return) {
  Initialised(
    state: initialised.state,
    selector: initialised.selector,
    return:,
    injected: initialised.injected,
  )
}

/// Handle `message` before anything in the mailbox.
///
/// This is gen_server's `handle_continue`: the initialiser returns at once,
/// so `start` unblocks, the supervisor moves on and the name is registered,
/// and the expensive setup still runs before the first external message.
/// The guarantee is not statistical — the queue is filled before the
/// acknowledgement that releases `start`, and the loop never looks at the
/// mailbox while the queue is non-empty.
///
/// Several calls run in the order they were written, all of them before the
/// mailbox. A continue handler may inject more with `then_handle`.
///
/// ## Examples
///
/// ```gleam
/// // `LoadIndex` is handled before any request the parent sends after
/// // `start` returns.
/// actor.new_with_initialiser(1000, fn(subject) {
///   actor.initialised(Empty)
///   |> actor.returning(subject)
///   |> actor.continuing(LoadIndex)
///   |> Ok
/// })
/// ```
pub fn continuing(
  initialised: Initialised(state, message, return),
  message: message,
) -> Initialised(state, message, return) {
  Initialised(
    ..initialised,
    injected: list.append(initialised.injected, [
      message,
    ]),
  )
}

// ---------------------------------------------------------------- builder

/// A description of an actor, ready to `start` or to hand to a supervisor.
///
/// Built with `new` or `new_with_initialiser` and refined with the setters
/// below.
pub opaque type Builder(state, message, return) {
  Builder(
    /// Run in the new process before the parent is told the actor started.
    /// An error here fails the start and is reported to the parent.
    initialise: fn(Subject(message)) ->
      Result(Initialised(state, message, return), String),
    /// How long the initialiser has, in milliseconds, before the actor is
    /// killed and the start reported as `InitTimeout`.
    initialisation_timeout: Int,
    /// Called for each message the actor receives.
    on_message: fn(state, message) -> Next(state, message),
    /// The name to register the actor under, if any.
    name: Option(process.Name(message)),
  )
}

/// Describe an actor with no custom initialisation.
///
/// The actor hands the parent a subject to send messages on — a named
/// subject if `named` was used.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(started) =
///   actor.new([]) |> actor.on_message(handle) |> actor.start
/// process.send(started.data, Push("Joe"))
/// ```
pub fn new(state: state) -> Builder(state, message, Subject(message)) {
  let initialise = fn(subject) {
    initialised(state) |> returning(subject) |> Ok
  }
  Builder(
    initialise:,
    initialisation_timeout: 1000,
    on_message: fn(state, _message) { continue(state) },
    name: None,
  )
}

/// Describe an actor with initialisation that runs in the new process
/// before `start` returns.
///
/// `timeout` is how many milliseconds the initialiser has; overrunning it
/// kills the actor and fails the start with `InitTimeout`. The actor's
/// default subject is passed in — return it to the parent with `returning`,
/// use it some other way, or ignore it.
///
/// Work that does not have to happen before `start` returns should not:
/// `continuing` runs it after the acknowledgement and still before the
/// first external message, which is what a supervisor's start timeout wants.
///
/// ## Examples
///
/// ```gleam
/// actor.new_with_initialiser(1000, fn(subject) {
///   use table <- result.try(open_table())
///   actor.initialised(table) |> actor.returning(subject) |> Ok
/// })
/// |> actor.on_message(handle)
/// |> actor.start
/// ```
pub fn new_with_initialiser(
  timeout: Int,
  initialise: fn(Subject(message)) ->
    Result(Initialised(state, message, return), String),
) -> Builder(state, message, return) {
  Builder(
    initialise:,
    initialisation_timeout: timeout,
    on_message: fn(state, _message) { continue(state) },
    name: None,
  )
}

/// Set the message handler.
///
/// The actor handles messages one at a time, in the order it receives them,
/// with injected messages first — see this module's header.
///
/// ## Examples
///
/// ```gleam
/// actor.new(0) |> actor.on_message(fn(count, _) { actor.continue(count + 1) })
/// ```
pub fn on_message(
  builder: Builder(state, message, return),
  handler: fn(state, message) -> Next(state, message),
) -> Builder(state, message, return) {
  Builder(..builder, on_message: handler)
}

/// Register the actor under `name` when it starts, so it can be reached by
/// a named subject rather than by passing a subject around.
///
/// If the name is already registered the actor fails to start. When this is
/// used the actor's default subject is the named one, which is what lets a
/// restarted actor take over from the one it replaced.
///
/// ## Examples
///
/// ```gleam
/// let name = process.new_name("cache")
/// let assert Ok(_) =
///   actor.new(dict.new()) |> actor.named(name) |> actor.start
/// process.send(process.named_subject(name), Put("k", "v"))
/// ```
pub fn named(
  builder: Builder(state, message, return),
  name: process.Name(message),
) -> Builder(state, message, return) {
  Builder(..builder, name: Some(name))
}

// ------------------------------------------------------------------ start

/// Start an actor from a builder.
///
/// The new process is linked to the caller, and the caller blocks until the
/// initialiser has finished or the initialisation timeout expires. Messages
/// added with `continuing` are handled after this returns and before any
/// message sent afterwards.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(started) =
///   actor.new(0) |> actor.on_message(handle) |> actor.start
/// // -> `started.data` is a subject; `started.pid` is the actor
/// ```
pub fn start(
  builder: Builder(state, message, return),
) -> Result(Started(return), StartError) {
  let timeout = builder.initialisation_timeout
  let ack_subject = process.new_subject()
  let parent = process.self()

  let child =
    process.spawn(fn() { initialise_actor(builder, parent, ack_subject) })

  // The monitor exists so that a child dying during initialisation is a
  // reported start failure rather than a wait for a timeout that will never
  // be answered.
  let monitor = process.monitor(child)
  let selector =
    process.new_selector()
    |> process.select_map(ack_subject, Ack)
    |> process.select_specific_monitor(monitor, Died)

  let result = case process.selector_receive(selector, timeout) {
    Ok(Ack(Ok(data))) -> Ok(data)
    Ok(Ack(Error(reason))) -> Error(otp_actor.InitFailed(reason))
    Ok(Died(down)) -> Error(otp_actor.InitExited(down.reason))

    // The child is unlinked before it is killed so that the parent is told
    // only by this error, and not also by an exit signal it did not ask for.
    Error(Nil) -> {
      process.unlink(child)
      process.kill(child)
      Error(otp_actor.InitTimeout)
    }
  }

  // The monitor has done its job; leaving it in place would deliver a
  // surprise `Down` to the parent if the actor died later.
  process.demonitor_process(monitor)

  case result {
    Ok(data) -> Ok(otp_actor.Started(pid: child, data:))
    Error(error) -> Error(error)
  }
}

/// What `start` is waiting for: the child's acknowledgement, or its death.
type StartEvent(data) {
  Ack(Result(data, String))
  Died(process.Down)
}

/// Describe this actor as a supervisor's child.
///
/// Returns `gleam/otp/supervision`'s own `ChildSpecification`, so a weft
/// actor is added to a `gleam_otp` supervisor exactly as an upstream one is.
/// The default is a permanent worker with a five-second shutdown; refine it
/// with `supervision.restart`, `supervision.timeout` and friends.
///
/// ## Examples
///
/// ```gleam
/// supervisor.new(supervisor.OneForOne)
/// |> supervisor.add(actor.supervised(cache_builder))
/// |> supervisor.start
/// ```
pub fn supervised(
  builder: Builder(state, message, return),
) -> supervision.ChildSpecification(return) {
  supervision.worker(fn() { start(builder) })
}

/// Send a message to an actor.
///
/// A re-export of `process.send`, for convenience.
///
/// ## Examples
///
/// ```gleam
/// actor.send(started.data, Push("Joe"))
/// ```
pub fn send(subject: Subject(message), message: message) -> Nil {
  process.send(subject, message)
}

/// Send a message and wait for the reply.
///
/// The caller crashes if no reply arrives within the timeout, rather than
/// carrying on against a process that may be in an unknown state. A
/// re-export of `process.call`.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok("Robert") = actor.call(subject, waiting: 10, sending: Pop)
/// ```
pub fn call(
  subject: Subject(message),
  waiting timeout: Int,
  sending make_message: fn(Subject(reply)) -> message,
) -> reply {
  process.call(subject, timeout, make_message)
}

// ------------------------------------------------------------------- loop

/// Everything the receive loop carries between messages.
type Self(state, message) {
  Self(
    /// The OTP debug plane: parent, mode, and the answers to `sys`.
    plane: sys.Plane,
    /// The state the programmer's handler sees.
    state: state,
    /// The programmer's selector, already wrapped into `Event`.
    selector: Selector(Event(message)),
    /// Injected messages awaiting handling, in the order they run. While
    /// this is non-empty the mailbox is not consulted for user messages.
    queue: List(message),
    /// The programmer's message handler.
    handler: fn(state, message) -> Next(state, message),
  )
}

/// Everything the loop can receive, in one type, so that a single selector
/// covers the mailbox.
type Event(message) {
  /// A message the programmer's selector accepted.
  Received(message: message)

  /// Something on the `system` tag: a debug request, or a request weft does
  /// not implement.
  System(incoming: sys.Incoming)

  /// A message no selector arm claimed. Discarded with a warning.
  Unexpected(message: Dynamic)
}

/// Run the actor's initialisation in the newly spawned process and, if it
/// succeeds, hand over to the loop.
///
/// The order in this function is the whole `continuing` guarantee. The
/// injected queue is populated from the `Initialised` value *before* the
/// acknowledgement is sent, and the loop drains the queue before it looks
/// at the mailbox. So however the parent races — handing the subject to a
/// client the instant `start` returns — no external message can be handled
/// first. Moving the acknowledgement earlier would break it; so would
/// filling the queue from anywhere but here.
fn initialise_actor(
  builder: Builder(state, message, return),
  parent: Pid,
  ack: Subject(Result(return, String)),
) -> ExitReason {
  let started = {
    use subject <- result.try(case builder.name {
      None -> Ok(process.new_subject())
      Some(name) -> {
        use _ <- result.try(register_self(name))
        Ok(process.named_subject(name))
      }
    })
    use initialised <- result.try(builder.initialise(subject))
    Ok(#(subject, initialised))
  }

  case started {
    // Initialisation failed. The parent is told why, and this process exits
    // normally: the failure is the parent's to report, not a crash to
    // propagate along the link.
    Error(reason) -> {
      process.send(ack, Error(reason))
      exit_process(Normal)
    }

    Ok(#(subject, Initialised(state:, selector:, return:, injected:))) -> {
      let selector = case selector {
        Some(selector) -> selector
        None -> process.new_selector() |> process.select(subject)
      }

      let self =
        Self(
          plane: sys.new(module: "weft@actor", parent:),
          state:,
          selector: process.map_selector(selector, Received),
          queue: injected,
          handler: builder.on_message,
        )

      // The queue is already loaded, so releasing the parent here cannot
      // let an external message overtake a continue.
      process.send(ack, Ok(return))
      loop(self)
    }
  }
}

/// Register the process under a name, turning a clash into the string the
/// parent will see as `InitFailed`.
fn register_self(name: process.Name(message)) -> Result(Nil, String) {
  case process.register(process.self(), name) {
    Ok(Nil) -> Ok(Nil)
    Error(Nil) -> Error("name already registered")
  }
}

/// The receive loop.
///
/// Suspension is checked first because it overrides everything else: a
/// suspended actor may not handle mailbox messages, may not drain its
/// injected queue, and may not time out. `sys:suspend/1` promises its caller
/// a frozen process, and the only way to keep that promise is to serve
/// nothing but the debug plane until `resume` arrives.
fn loop(self: Self(state, message)) -> ExitReason {
  case sys.is_suspended(self.plane) {
    True -> {
      let incoming = process.selector_receive_forever(system_selector())
      loop(handle_system(self, incoming))
    }
    False -> run(self)
  }
}

/// The running half of the loop: injected work first, then the mailbox.
fn run(self: Self(state, message)) -> ExitReason {
  case self.queue {
    [] -> await_message(self)

    [message, ..rest] ->
      // The debug plane is checked between every two injected messages, so
      // a `suspend` arriving mid-chain takes effect here rather than after
      // the chain drains. Handling one and going back through `loop` rather
      // than draining them all is deliberate: the mode change has to be
      // observed before the next injected message runs, or a suspended
      // actor would keep working.
      case poll_system(self) {
        Some(self) -> loop(self)
        None -> handle(Self(..self, queue: rest), message)
      }
  }
}

/// Block for the next message.
fn await_message(self: Self(state, message)) -> ExitReason {
  dispatch(self, process.selector_receive_forever(running_selector(self)))
}

/// Take one already-arrived system message without blocking, if there is
/// one.
///
/// The scan is over the whole mailbox, as any selective receive is, and it
/// happens once per injected message; a long continue chain in front of a
/// deep mailbox pays for that. The alternative — draining the queue first
/// and reading system messages after — would let a continue chain starve
/// the debug plane, which is the failure this ordering exists to prevent.
fn poll_system(self: Self(state, message)) -> Option(Self(state, message)) {
  case process.selector_receive(system_selector(), 0) {
    Error(Nil) -> None
    Ok(incoming) -> Some(handle_system(self, incoming))
  }
}

/// The selector for a suspended actor, and for the mid-chain poll: the
/// debug plane and nothing else.
fn system_selector() -> Selector(sys.Incoming) {
  process.new_selector() |> sys.selecting(fn(incoming) { incoming })
}

/// The selector for a running actor.
///
/// The arms weft owns go on first so that a programmer's selector can
/// deliberately replace them. The system arm goes on last, where nothing
/// can shadow it: an
/// actor invisible to `sys` is a debugging dead end, and that is not a
/// choice worth offering.
fn running_selector(self: Self(state, message)) -> Selector(Event(message)) {
  process.new_selector()
  |> process.select_other(Unexpected)
  |> process.merge_selector(self.selector)
  |> sys.selecting(System)
}

/// Act on one received event.
fn dispatch(self: Self(state, message), event: Event(message)) -> ExitReason {
  case event {
    System(incoming:) -> loop(handle_system(self, incoming))

    // Not selected for by anyone, so the programmer has not accounted for
    // it. Discarding it silently would make a mis-wired selector look like
    // a hung actor.
    Unexpected(message:) -> {
      sys.warn(
        "weft/actor discarding unexpected message: " <> string.inspect(message),
      )
      loop(self)
    }

    Received(message:) -> handle(self, message)
  }
}

/// Answer the debug plane.
fn handle_system(
  self: Self(state, message),
  incoming: sys.Incoming,
) -> Self(state, message) {
  case incoming {
    sys.Unimplemented(request:) -> {
      sys.warn(
        "weft/actor received an unimplemented system message: "
        <> string.inspect(request),
      )
      self
    }

    sys.Request(message:) -> {
      let plane = sys.handle(self.plane, message, holding: self.state)
      Self(..self, plane:)
    }
  }
}

/// Run the programmer's handler for one message.
fn handle(self: Self(state, message), message: message) -> ExitReason {
  case self.handler(self.state, message) {
    Stop(reason:) -> exit_process(reason)

    Continue(state:, selector:, injected:) -> {
      let selector = case selector {
        None -> self.selector
        Some(selector) -> process.map_selector(selector, Received)
      }

      // Depth first: what this handler injected goes in front of what was
      // already queued, in the order it was written. The block form is what
      // makes `then_handle` behave like gen_statem's `next_event` rather
      // than like a self-send.
      let queue = list.append(injected, self.queue)
      loop(Self(..self, state:, selector:, queue:))
    }
  }
}

/// Exit the process with a reason.
///
/// `Normal` needs no signal — returning from the loop is enough — but an
/// abnormal reason has to be signalled so that it propagates along links,
/// and a kill has to be a kill.
fn exit_process(reason: ExitReason) -> ExitReason {
  case reason {
    Normal -> Nil
    Abnormal(reason:) -> process.send_abnormal_exit(process.self(), reason)
    Killed -> process.kill(process.self())
  }
  reason
}
