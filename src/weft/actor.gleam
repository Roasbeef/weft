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
//// own that place. The same is true of the three smaller gaps that ride
//// along — a `terminate/2` analog, hibernation, and a loop timeout — each
//// of which is a decision made between `receive` and the callback. So weft
//// reimplements the loop and keeps the surface identical, which is the
//// trade that makes migration a change of import line.
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
import weft/internal/timer

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
    /// Whether the actor traps exits, turning exit signals from linked
    /// processes into messages the loop can act on.
    trap_exits: Bool,
    /// Called on the way out, if the actor gets a chance. See `on_shutdown`
    /// for exactly when that is.
    on_shutdown: Option(fn(state, ExitReason) -> Nil),
    /// How long the actor may sit idle, in milliseconds, before hibernating.
    hibernate_after: Option(Int),
    /// The loop timeout: a message to handle after a quiet period.
    idle_timeout: Option(Idle(message)),
  )
}

/// The loop timeout configuration: how long quiet counts as idle, and what
/// the actor sends itself when it does.
type Idle(message) {
  Idle(after_ms: Int, message: message)
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
    trap_exits: False,
    on_shutdown: None,
    hibernate_after: None,
    idle_timeout: None,
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
    trap_exits: False,
    on_shutdown: None,
    hibernate_after: None,
    idle_timeout: None,
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

/// Choose whether the actor traps exits.
///
/// A trapping actor receives an exit signal from a linked process as a
/// message instead of dying of it, which is what makes an orderly shutdown
/// possible; it is the prerequisite for `on_shutdown` firing on anything
/// other than the actor's own `stop`.
///
/// The policy the loop applies to a trapped exit mirrors what would have
/// happened without trapping, plus the chance to run `on_shutdown` first:
/// an exit from the parent shuts the actor down whatever the reason —
/// including `Normal`, as OTP behaviours do, because a child outliving its
/// parent is a leak — and an exit from any other linked process shuts it
/// down only if the reason is abnormal.
///
/// Trapping is off by default, and turning it on changes what kills the
/// actor: `process.kill` still does, `process.send_exit` no longer does.
///
/// ## Examples
///
/// ```gleam
/// actor.new(state)
/// |> actor.trapping_exits(True)
/// |> actor.on_shutdown(fn(state, _reason) { close(state.handle) })
/// ```
pub fn trapping_exits(
  builder: Builder(state, message, return),
  trap: Bool,
) -> Builder(state, message, return) {
  Builder(..builder, trap_exits: trap)
}

/// Run `handler` with the final state as the actor shuts down.
///
/// This is a `terminate/2` analog and it is **best effort**. Read the list
/// before relying on it:
///
/// - It runs when a handler returns `stop` or `stop_abnormal`, with that
///   reason.
/// - It runs when a trapped exit signal shuts the actor down, with the
///   reason from the signal — but only if `trapping_exits(True)` was set.
///   Without trapping, an exit signal kills the process directly and no
///   Gleam code runs.
/// - It never runs when the actor is killed with an untrappable signal
///   (`process.kill`, or a supervisor's brutal kill after a shutdown
///   timeout). Nothing runs then; that is what untrappable means.
/// - It never runs if initialisation fails, because there is no state to
///   hand it.
/// - It does not run if the actor's own handler crashes.
///
/// So it is the right place to release something whose loss is an
/// inconvenience, and the wrong place for anything whose loss is a
/// correctness problem. If it must happen, it belongs with a process that
/// monitors this one, not here.
///
/// The reason passed may itself be `Killed` — that is a *linked* process
/// having been killed, which this actor was told about and is shutting down
/// because of, not this actor being killed.
///
/// ## Examples
///
/// ```gleam
/// actor.new(state)
/// |> actor.trapping_exits(True)
/// |> actor.on_shutdown(fn(state, reason) {
///   log("closing " <> state.name <> ": " <> string.inspect(reason))
/// })
/// ```
pub fn on_shutdown(
  builder: Builder(state, message, return),
  handler: fn(state, ExitReason) -> Nil,
) -> Builder(state, message, return) {
  Builder(..builder, on_shutdown: Some(handler))
}

/// Hibernate the actor after `ms` milliseconds with no messages.
///
/// Hibernation runs a full garbage collection and drops the process stack,
/// leaving the actor at its minimum footprint until the next message wakes
/// it. It is for the ten-thousand-mostly-idle-actors case, where the
/// aggregate heap matters more than the microseconds a wake-up costs; an
/// actor that receives steadily should not be given it.
///
/// The mechanism is a receive timeout rather than a timer message, so
/// nothing is ever queued and there is no stale fire to discard: the actor
/// hibernates from inside the receive it was already blocked in, and
/// `erlang:hibernate/3` resumes it into the same loop with the same state
/// when a message arrives.
///
/// ## Examples
///
/// ```gleam
/// actor.new(state)
/// |> actor.hibernate_after(60_000)
/// |> actor.on_message(handle)
/// ```
pub fn hibernate_after(
  builder: Builder(state, message, return),
  ms: Int,
) -> Builder(state, message, return) {
  Builder(..builder, hibernate_after: Some(ms))
}

/// Handle `message` after `ms` milliseconds with no messages.
///
/// This is gen_server's loop timeout: the clock is reset every time the
/// actor handles a message, so it fires only after a genuinely quiet
/// stretch, and the message is an ordinary one delivered to the ordinary
/// handler.
///
/// The mechanism is a named timer from `weft/internal/timer` rather than a
/// receive timeout, because an actor may also be hibernating, and a receive
/// cannot have two deadlines. That choice brings a race with it, and the
/// race is closed rather than tolerated: the timer can fire in the instant
/// between a real message arriving and the actor resetting the clock, which
/// would put a timeout message in the mailbox that no longer describes
/// anything true. Every arming carries a generation stamp, and the timer
/// book drops a fire whose stamp is no longer current, so a message already
/// in flight when the clock is reset is discarded before it reaches the
/// handler. The visible behaviour is the one the name promises: traffic
/// cancels the timeout.
///
/// The timeout is not armed while the actor is suspended by `sys:suspend/1`
/// — a suspended actor is not idle, it is frozen — and is re-armed on
/// resume.
///
/// ## Examples
///
/// ```gleam
/// // A connection that closes itself after five minutes of quiet.
/// actor.new(connection)
/// |> actor.idle_timeout(300_000, IdleTooLong)
/// |> actor.on_message(fn(state, message) {
///   case message {
///     IdleTooLong -> actor.stop()
///     Request(r) -> actor.continue(serve(state, r))
///   }
/// })
/// ```
pub fn idle_timeout(
  builder: Builder(state, message, return),
  ms: Int,
  message: message,
) -> Builder(state, message, return) {
  Builder(..builder, idle_timeout: Some(Idle(after_ms: ms, message:)))
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
    /// The best-effort shutdown callback, if configured.
    on_shutdown: Option(fn(state, ExitReason) -> Nil),
    /// Whether exit signals arrive as messages. Decides whether the loop
    /// installs the trapped-exit arm at all.
    trapping: Bool,
    /// The quiet period after which the actor hibernates, if configured.
    hibernate_after: Option(Int),
    /// The loop timeout, if configured.
    idle: Option(Idle(message)),
    /// The timer book backing the loop timeout, and the flush that makes a
    /// cancelled timeout safe.
    timers: timer.Timers(TimerKey, message),
  )
}

/// The keys the actor arms timers under. There is one; the type exists so
/// that the timer book's key type is a name rather than a bare atom, and so
/// that a second timeout later cannot be confused with this one.
type TimerKey {
  IdleTimer
}

/// Everything the loop can receive, in one type, so that a single selector
/// covers the mailbox.
type Event(message) {
  /// A message the programmer's selector accepted.
  Received(message: message)

  /// Something on the `system` tag: a debug request, or a request weft does
  /// not implement.
  System(incoming: sys.Incoming)

  /// An exit signal from a linked process, arriving as a message because
  /// the actor traps exits.
  Trapped(exit: process.ExitMessage)

  /// The loop timeout fired. It may be stale; the timer book decides.
  Tick(fired: timer.Fired(TimerKey, message))

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
  // Trapping is established before the initialiser runs, so that a link
  // made during initialisation is already covered by it.
  case builder.trap_exits {
    True -> process.trap_exits(True)
    False -> Nil
  }

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
          on_shutdown: builder.on_shutdown,
          trapping: builder.trap_exits,
          hibernate_after: builder.hibernate_after,
          idle: builder.idle_timeout,
          timers: timer.new(process.new_subject()),
        )

      // The queue is already loaded, so releasing the parent here cannot
      // let an external message overtake a continue.
      process.send(ack, Ok(return))
      loop(arm_idle(self))
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

/// What a suspended actor is still willing to receive.
///
/// Two things, and no more. The debug plane, because `resume` arrives on it
/// and a suspension nothing can lift is a hang. And, for an actor that traps
/// exits, an exit signal — because a supervisor shutting down a suspended
/// child would otherwise wait out its whole shutdown timeout and then kill
/// it, which is the outcome trapping exists to avoid. OTP's own
/// `sys:suspend_loop` makes the same exception for the same reason.
type Frozen {
  FrozenSystem(incoming: sys.Incoming)
  FrozenExit(exit: process.ExitMessage)
}

/// The receive loop.
///
/// Suspension is checked first because it overrides everything else: a
/// suspended actor may not handle mailbox messages, may not drain its
/// injected queue, and may not time out. `sys:suspend/1` promises its caller
/// a frozen process, and the only way to keep that promise is to serve
/// nothing but the debug plane — and its own death — until `resume` arrives.
fn loop(self: Self(state, message)) -> ExitReason {
  case sys.is_suspended(self.plane) {
    True ->
      case process.selector_receive_forever(frozen_selector(self.trapping)) {
        FrozenSystem(incoming:) -> loop(handle_system(self, incoming))
        FrozenExit(exit:) -> handle_exit(self, exit)
      }
    False -> run(self)
  }
}

/// The selector for a suspended actor.
fn frozen_selector(trapping: Bool) -> Selector(Frozen) {
  let selector = process.new_selector() |> sys.selecting(FrozenSystem)
  case trapping {
    False -> selector
    True -> process.select_trapped_exits(selector, FrozenExit)
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

/// Block for the next message, hibernating first if the actor has been
/// quiet for long enough.
fn await_message(self: Self(state, message)) -> ExitReason {
  let selector = running_selector(self)
  case self.hibernate_after {
    None -> dispatch(self, process.selector_receive_forever(selector))

    Some(ms) ->
      case process.selector_receive(selector, ms) {
        Ok(event) -> dispatch(self, event)

        // Quiet for long enough: shed the stack and heap and resume into
        // the same loop when a message arrives. This call does not return,
        // which is safe here only because the loop's exit is a signal sent
        // by `exit_process` rather than a value returned up this stack.
        Error(Nil) -> sys.hibernate(fn() { loop(self) })
      }
  }
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
/// deliberately replace them — most usefully the trapped-exit arm, for an
/// actor that wants to see exits as its own messages rather than as
/// shutdowns. The system arm goes on last, where nothing can shadow it: an
/// actor invisible to `sys` is a debugging dead end, and that is not a
/// choice worth offering.
fn running_selector(self: Self(state, message)) -> Selector(Event(message)) {
  process.new_selector()
  |> process.select_other(Unexpected)
  |> select_exits(self.trapping)
  |> process.select_map(timer.subject(self.timers), Tick)
  |> process.merge_selector(self.selector)
  |> sys.selecting(System)
}

/// Add the trapped-exit arm, but only for an actor that traps: without
/// trapping no such message can arrive, and installing the arm would
/// suggest otherwise.
fn select_exits(
  selector: Selector(Event(message)),
  trapping: Bool,
) -> Selector(Event(message)) {
  case trapping {
    False -> selector
    True -> process.select_trapped_exits(selector, Trapped)
  }
}

/// Act on one received event.
fn dispatch(self: Self(state, message), event: Event(message)) -> ExitReason {
  case event {
    System(incoming:) -> loop(handle_system(self, incoming))

    Trapped(exit:) -> handle_exit(self, exit)

    Tick(fired:) -> handle_tick(self, fired)

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

/// Answer the debug plane, and keep the idle timeout consistent with the
/// mode.
///
/// A suspended actor is frozen, not idle, so its loop timeout is disarmed
/// on the way in and re-armed on the way out. Leaving it armed would fire a
/// timeout for a quiet period the actor was not allowed to act in.
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
      let was_suspended = sys.is_suspended(self.plane)
      let plane = sys.handle(self.plane, message, holding: self.state)
      let self = Self(..self, plane:)
      case was_suspended, sys.is_suspended(plane) {
        False, True ->
          Self(..self, timers: timer.cancel(self.timers, IdleTimer))
        True, False -> arm_idle(self)
        False, False -> self
        True, True -> self
      }
    }
  }
}

/// Decide what a trapped exit signal means.
///
/// The parent's exit takes the actor with it whatever the reason, which is
/// what every OTP behaviour does: a child that outlives the process that
/// started it is a leak, and `Normal` is not an exception to that. Any other
/// linked process is treated as the link itself would have treated it —
/// ignored if it exited normally, fatal otherwise — so that turning trapping
/// on to get `on_shutdown` does not quietly change the topology.
fn handle_exit(
  self: Self(state, message),
  exit: process.ExitMessage,
) -> ExitReason {
  case exit.pid == sys.parent(self.plane) {
    True -> shutdown(self, exit.reason)

    False ->
      case exit.reason {
        Normal -> loop(self)
        Killed -> shutdown(self, Killed)
        Abnormal(reason:) -> shutdown(self, Abnormal(reason:))
      }
  }
}

/// Deliver a timer message, or drop it.
///
/// This is the consumer half of the timer book's cancel-with-flush: a fire
/// whose arming has been cancelled or replaced dies here, and never reaches
/// the programmer's handler.
fn handle_tick(
  self: Self(state, message),
  fired: timer.Fired(TimerKey, message),
) -> ExitReason {
  case timer.accept(self.timers, fired) {
    timer.Stale(timers:) -> loop(Self(..self, timers:))
    timer.Deliver(timers:, key: IdleTimer, message:) ->
      handle(Self(..self, timers:), message)
  }
}

/// Run the programmer's handler for one message.
fn handle(self: Self(state, message), message: message) -> ExitReason {
  case self.handler(self.state, message) {
    Stop(reason:) -> shutdown(self, reason)

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
      loop(arm_idle(Self(..self, state:, selector:, queue:)))
    }
  }
}

/// Re-arm the loop timeout, if there is one.
///
/// Called after every handled message, which is what "reset by traffic"
/// means, and once at start. Re-arming cancels the previous timer; a fire
/// that beat the cancel into the mailbox is stamped with the old generation
/// and is discarded by `handle_tick`.
fn arm_idle(self: Self(state, message)) -> Self(state, message) {
  case self.idle {
    None -> self
    Some(Idle(after_ms:, message:)) ->
      Self(
        ..self,
        timers: timer.set(
          self.timers,
          for: IdleTimer,
          after: after_ms,
          sending: message,
        ),
      )
  }
}

/// Run the shutdown callback, then exit.
///
/// The callback runs before the exit signal so that it observes the state
/// the actor actually stopped with, and so that anything it releases is
/// released before linked processes are told. It is not protected: a
/// callback that crashes takes the actor down with a different reason than
/// the one it was shutting down with, which is why `on_shutdown` documents
/// itself as best effort.
fn shutdown(self: Self(state, message), reason: ExitReason) -> ExitReason {
  case self.on_shutdown {
    None -> Nil
    Some(handler) -> handler(self.state, reason)
  }
  exit_process(reason)
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
