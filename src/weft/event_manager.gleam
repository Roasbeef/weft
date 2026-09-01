//// A typed `gen_event`: one process holding an ordered list of handlers,
//// each carrying its own private state.
////
//// ## The problem, and the encoding that answers it
////
//// `gleam_otp` has no `gen_event` binding and probably never will, because
//// gen_event's central structure is a list in which every element has a
//// *different* state type. A token counter holds an `Int`, a transcript
//// holds a file handle, and gen_event keeps both in one list. Gleam has no
//// direct representation for that list, which is exactly why the binding is
//// missing upstream.
////
//// The way out is to stop asking the handler to expose its state. A handler
//// is a function from an event to its own **successor**, and the state lives
//// in the successor's closure where it never reaches the type:
////
//// ```gleam
//// Handler(step: fn(event) -> Outcome(event))
//// ```
////
//// `List(Handler(event))` is now homogeneous while every handler in it is
//// still as differently-stated as gen_event's. `handler` writes the
//// recursion for you from ordinary state-threading code, so the closure
//// trick is an implementation detail of this module rather than something
//// callers perform by hand.
////
//// The encoding buys one thing Erlang's does not have. A handler that is
//// broken has to *say why*, because `Result(state, String)` is the return
//// type its author is given; in Erlang a handler simply crashes and is
//// silently swapped out.
////
//// ## What the manager guarantees
////
//// - Handlers run in the order they were added, and a successor takes its
////   predecessor's place in the list, so the order is stable across events.
////   `add_handler` at runtime appends, so a late handler runs last.
//// - A handler returning `Failed` is removed and the reason is logged. Its
////   siblings do not see the removal at all: they were run before it or are
////   run after it, from the same fan-out.
//// - `sync_notify` returns only once every handler has finished with the
////   event. It is a `call` whose reply the manager sends after the fan-out,
////   so a caller that needs "the effects are visible now" — a test, a
////   shutdown ordering, backpressure — gets it without sleeping.
////
//// ## Isolation, and its honest limit
////
//// Handlers run *in the manager's process*, as gen_event's do. Two things
//// follow, and only one of them is shared with OTP.
////
//// A slow handler stalls the manager and everything behind it. gen_event has
//// the same property, and it is the price of the ordering above.
////
//// A handler that *raises* — a `panic`, an assertion that does not hold, a
//// badmatch inside the closure — takes the manager down with it. Here weft
//// is weaker than Erlang, which wraps every callback in a try/catch, removes
//// the offender and carries on. gen_event catches because its callbacks are
//// dynamically typed and it has no other way to be told about a broken one;
//// weft asks for the failure in the type instead, and `Failed` is isolated
//// exactly as gen_event's removal is. What weft does not do is rescue a
//// handler that does not *know* it is broken, because Gleam has no exception
//// handling and the rescue would have to be FFI. So: a raising handler is a
//// manager crash, and the answer to it is a supervisor, not a `Failed`.
//// Better to say that out loud than to let a reader infer an isolation that
//// is not there.
////
//// ## What is deliberately not here
////
//// **Per-handler `call`.** `gen_event:call(Manager, HandlerId, Request)` has
//// no honest type, because the request and reply types differ per handler.
//// The answer is that a handler which needs to be queried is not a handler,
//// it is an actor that happens to subscribe. Hand it a `Subject` of its own
//// and let callers talk to it directly. This is not a gap to be filed; it is
//// a thing the type system is right to refuse.
////
//// **Handler supervision.** `add_sup_handler` and the `gen_event_EXIT`
//// message are out for v1. The `Failed`-drop-and-log path covers what the
//// removal notification is usually used for; an `on_handler_exit` builder
//// option waits for a real consumer rather than being guessed at now.
////
//// **Handler identity.** `add_handler` returns `Nil`, so there is no
//// `HandlerRef` and no `delete_handler(ref)`. `RemoveSelf` covers removal
//// from the inside, which is every case seen so far. External removal, and
//// the reference type it would need, are deferred until something actually
//// wants them.
////
//// ## Example
////
//// A session bus where one handler counts tokens and another accumulates a
//// transcript. Neither state type is visible outside its constructor, and
//// both are in the same list.
////
//// ```gleam
//// let assert Ok(started) =
////   event_manager.new()
////   |> event_manager.add(event_manager.handler(0, on_event: count_tokens))
////   |> event_manager.add(event_manager.handler(transcript, on_event: append))
////   |> event_manager.start
////
//// event_manager.notify(started.data, Token("hello"))
////
//// // A full disk removes the transcript with `Failed`; the counter keeps
//// // counting, and this returns 1.
//// event_manager.count_handlers(started.data, waiting: 1000)
//// ```

import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/supervision
import weft/actor

// The one internal module this one reaches for, and only for `warn`.
// Dropping a handler must leave a trace — a silent discard is how a bus goes
// quiet for a week before anybody notices — and `sys.warn` is already the
// house wrapper over OTP's logger that `weft/actor` discards unexpected
// messages through. Nothing else from `weft/internal/sys` belongs here: the
// system-message plane is the actor loop's business, and this module is a
// client of that loop like any other.
import weft/internal/sys

// ---------------------------------------------------------------- handlers

/// One handler in a manager's list, with its state sealed inside a closure.
///
/// Only `event` appears in the type, which is what lets handlers with
/// unrelated state types share a `List(Handler(event))`. Build one with
/// `handler`, or with `handler_with_outcome` when the handler needs to
/// remove itself.
pub opaque type Handler(event) {
  Handler(step: fn(event) -> Outcome(event))
}

/// What a handler has decided, having seen one event.
///
/// The manager acts on this and nothing else: a handler cannot reach the
/// manager's list, cannot see its siblings, and cannot stop the manager.
pub type Outcome(event) {
  /// Handled. Use this handler for the next event, in the same position.
  Keep(handler: Handler(event))

  /// Handled. Remove me from the manager; my siblings carry on unchanged.
  RemoveSelf

  /// This handler is broken and knows it. The manager drops it and logs the
  /// reason. Siblings and the manager itself are unaffected, and that
  /// isolation is gen_event's whole value.
  Failed(reason: String)
}

/// Build a handler from ordinary state-threading code.
///
/// `on_event` is written as if it owned a plain piece of state: take the
/// state and the event, return the next state, or an `Error` saying what
/// went wrong. The recursion that carries the state forward is written here,
/// in the successor's closure, so the state never escapes into the type.
///
/// An `Error` becomes `Failed`: the handler is removed and the reason
/// logged. A handler that wants to retire without having failed wants
/// `handler_with_outcome` and `RemoveSelf`.
///
/// ## Examples
///
/// ```gleam
/// // State is an `Int` and is invisible from outside.
/// let tokens =
///   event_manager.handler(0, on_event: fn(count, event) {
///     case event {
///       Token(_) -> Ok(count + 1)
///       Report(reply:) -> {
///         process.send(reply, count)
///         Ok(count)
///       }
///     }
///   })
/// ```
///
/// ```gleam
/// // The same shape, failing out of the list when the disk fills. The type
/// // forces the handler to say why it is going.
/// let transcript =
///   event_manager.handler(file, on_event: fn(file, event) {
///     case write(file, event) {
///       Ok(file) -> Ok(file)
///       Error(reason) -> Error("transcript write failed: " <> reason)
///     }
///   })
/// ```
pub fn handler(
  state: state,
  on_event handle: fn(state, event) -> Result(state, String),
) -> Handler(event) {
  Handler(step: fn(event) {
    // The successor closes over the *new* state, so the next event is
    // handled against it while the type of the handler is unchanged. This
    // is the whole encoding, in three lines.
    case handle(state, event) {
      Ok(state) -> Keep(handler(state, handle))
      Error(reason) -> Failed(reason)
    }
  })
}

/// Build a handler that returns its own `Outcome`.
///
/// This is `handler` without the state-threading convenience, and it exists
/// for the outcomes that convenience cannot express — `RemoveSelf` above
/// all, since a handler that has finished its work is not a handler that has
/// `Failed`. The caller writes the successor recursion by hand and gets
/// `RemoveSelf` in exchange.
///
/// ## Examples
///
/// ```gleam
/// // A handler that fires once and then retires.
/// let once =
///   event_manager.handler_with_outcome(fn(event) {
///     announce(event)
///     event_manager.RemoveSelf
///   })
/// ```
///
/// ```gleam
/// // Hand-written state threading, for a handler that also removes itself.
/// fn countdown(remaining: Int) -> event_manager.Handler(event) {
///   use _event <- event_manager.handler_with_outcome
///   case remaining {
///     0 -> event_manager.RemoveSelf
///     n -> event_manager.Keep(countdown(n - 1))
///   }
/// }
/// ```
pub fn handler_with_outcome(
  on_event step: fn(event) -> Outcome(event),
) -> Handler(event) {
  Handler(step:)
}

// ----------------------------------------------------------------- traffic

/// What a manager accepts.
///
/// Opaque, because every constructor has a function in this module that
/// sends it correctly — `notify`, `sync_notify`, `add_handler`,
/// `count_handlers` — and a hand-built `SyncNotify` with the wrong reply
/// subject is a hang rather than a type error. The type is public so that a
/// caller can name `process.Name(Message(event))` for `named`, and
/// `Subject(Message(event))` for whatever it stores the manager in.
pub opaque type Message(event) {
  /// Fan the event out to every handler. Nobody is waiting.
  Notify(event: event)

  /// Fan the event out, then answer `reply`. The reply is what makes the
  /// caller's wait mean "every handler has finished", so it is sent after
  /// the fan-out and never before it.
  SyncNotify(event: event, reply: Subject(Nil))

  /// Append a handler. It sees events sent after this message, and no
  /// earlier ones.
  AddHandler(handler: Handler(event))

  /// How many handlers are in the list right now.
  CountHandlers(reply: Subject(Int))
}

// ----------------------------------------------------------------- builder

/// A description of a manager, ready to `start` or to hand to a supervisor.
///
/// Built with `new` and refined with `add` and `named`.
pub opaque type Builder(event) {
  Builder(
    /// The handlers, in the order they will run.
    handlers: List(Handler(event)),
    /// The name to register the manager under, if any.
    name: Option(process.Name(Message(event))),
  )
}

/// Describe a manager with no handlers.
///
/// A manager with an empty list is useful rather than degenerate: every
/// `notify` is a no-op until something subscribes with `add_handler`, which
/// is the usual shape for a bus started by a supervisor before its consumers
/// exist.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(started) = event_manager.new() |> event_manager.start
/// event_manager.add_handler(started.data, audit_log)
/// ```
pub fn new() -> Builder(event) {
  Builder(handlers: [], name: None)
}

/// Add a handler to the manager being described.
///
/// Handlers run in the order they were added, so this is also the order the
/// fan-out will visit them in.
///
/// ## Examples
///
/// ```gleam
/// event_manager.new()
/// |> event_manager.add(token_counter)
/// |> event_manager.add(transcript)
/// ```
pub fn add(builder: Builder(event), handler: Handler(event)) -> Builder(event) {
  // Appending rather than prepending is what makes "the order they were
  // added" true of the builder as well as of the fan-out. The list is the
  // handful of handlers one bus has, so the walk is not worth optimising
  // into a reversal at start time.
  Builder(..builder, handlers: list.append(builder.handlers, [handler]))
}

/// Register the manager under `name` when it starts, so it can be reached by
/// a named subject rather than by passing a subject around.
///
/// If the name is already registered the manager fails to start. This is how
/// a supervised bus is reached: the supervisor holds the child, and everyone
/// else sends to `process.named_subject(name)`.
///
/// ## Examples
///
/// ```gleam
/// let name = process.new_name("session_bus")
/// let assert Ok(_) =
///   event_manager.new() |> event_manager.named(name) |> event_manager.start
/// event_manager.notify(process.named_subject(name), Token("hello"))
/// ```
pub fn named(
  builder: Builder(event),
  name: process.Name(Message(event)),
) -> Builder(event) {
  Builder(..builder, name: Some(name))
}

/// Start a manager from a builder.
///
/// The manager is a `weft/actor` whose state is the handler list, so the
/// process is linked to the caller and `started.data` is the subject to send
/// on, exactly as any other weft actor's is.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(started) =
///   event_manager.new()
///   |> event_manager.add(token_counter)
///   |> event_manager.start
/// event_manager.notify(started.data, Token("hello"))
/// ```
pub fn start(
  builder: Builder(event),
) -> actor.StartResult(Subject(Message(event))) {
  actor.start(actor_builder(builder))
}

/// Describe this manager as a supervisor's child.
///
/// Returns `gleam/otp/supervision`'s own `ChildSpecification`, by way of
/// `weft/actor`, so a manager is added to a `gleam_otp` supervisor exactly
/// as any other actor is. A supervised manager is nearly always a `named`
/// one, since a restart replaces the subject the caller was holding.
///
/// ## Examples
///
/// ```gleam
/// supervisor.new(supervisor.OneForOne)
/// |> supervisor.add(event_manager.supervised(bus_builder))
/// |> supervisor.start
/// ```
pub fn supervised(
  builder: Builder(event),
) -> supervision.ChildSpecification(Subject(Message(event))) {
  actor.supervised(actor_builder(builder))
}

/// Translate the description into the actor it is.
///
/// The manager has no initialisation to do — its whole state is the handler
/// list the builder already holds — so this is the plain `actor.new` path,
/// and the only choice left is whether a name was asked for.
fn actor_builder(
  builder: Builder(event),
) -> actor.Builder(
  List(Handler(event)),
  Message(event),
  Subject(Message(event)),
) {
  let described = actor.new(builder.handlers) |> actor.on_message(handle)
  case builder.name {
    None -> described
    Some(name) -> actor.named(described, name)
  }
}

// -------------------------------------------------------------- operations

/// Send an event to every handler, without waiting.
///
/// Returns as soon as the event is in the manager's mailbox, which is before
/// any handler has seen it. Use `sync_notify` when the caller needs the
/// handlers' effects to have happened.
///
/// ## Examples
///
/// ```gleam
/// event_manager.notify(bus, Token("hello"))
/// ```
pub fn notify(manager: Subject(Message(event)), event: event) -> Nil {
  actor.send(manager, Notify(event:))
}

/// Send an event to every handler and wait until all of them have handled it.
///
/// The manager replies after the fan-out, never before, so when this returns
/// every handler has run and every effect a handler performed is visible.
/// That makes it the backpressure variant — a producer that outruns the bus
/// blocks here — and the shutdown-ordering one, and it is why a test can
/// assert on a handler's effects with a zero timeout instead of a sleep.
///
/// `waiting` is milliseconds. As with `weft/actor`'s `call`, which this is,
/// the caller **crashes** if the manager does not reply in time rather than
/// carrying on against a process in an unknown state.
///
/// ## Examples
///
/// ```gleam
/// event_manager.sync_notify(bus, Token("hello"), waiting: 1000)
/// // -> every handler has now finished with the event
/// ```
pub fn sync_notify(
  manager: Subject(Message(event)),
  event: event,
  waiting timeout: Int,
) -> Nil {
  actor.call(manager, waiting: timeout, sending: fn(reply) {
    SyncNotify(event:, reply:)
  })
}

/// Add a handler to a running manager.
///
/// The handler is appended, so it runs last in the fan-out, and it sees only
/// events sent after this call — messages to one process from one process
/// arrive in order, so an event sent after this one cannot be handled before
/// the handler is in the list.
///
/// There is no handle to remove it by; see this module's header on handler
/// identity, and `RemoveSelf` for removal from the inside.
///
/// ## Examples
///
/// ```gleam
/// event_manager.add_handler(bus, audit_log)
/// // The audit log sees this event, and none before it.
/// event_manager.notify(bus, Token("hello"))
/// ```
pub fn add_handler(
  manager: Subject(Message(event)),
  handler: Handler(event),
) -> Nil {
  actor.send(manager, AddHandler(handler:))
}

/// How many handlers the manager currently holds.
///
/// The count falls as handlers remove themselves and as broken ones are
/// dropped, so it is the observable side of `RemoveSelf` and `Failed`.
///
/// `waiting` is milliseconds, and the caller crashes on timeout for the
/// reason given on `sync_notify`.
///
/// ## Examples
///
/// ```gleam
/// event_manager.count_handlers(bus, waiting: 1000)
/// // -> 2
/// ```
pub fn count_handlers(
  manager: Subject(Message(event)),
  waiting timeout: Int,
) -> Int {
  actor.call(manager, waiting: timeout, sending: CountHandlers)
}

// -------------------------------------------------------------------- loop

/// The manager's message handler: an ordinary actor handler whose state is
/// the handler list.
///
/// Every arm returns the list the fan-out produced rather than the one it
/// was given, which is how successors replace their predecessors and how
/// removals take effect.
fn handle(
  handlers: List(Handler(event)),
  message: Message(event),
) -> actor.Next(List(Handler(event)), Message(event)) {
  case message {
    Notify(event:) -> actor.continue(fan_out(handlers, event))

    SyncNotify(event:, reply:) -> {
      let handlers = fan_out(handlers, event)
      // The reply is what the caller's wait means. Sending it here, after
      // the fan-out has returned, is the whole of `sync_notify`'s contract;
      // moving it above the fan-out would turn the call into an expensive
      // `notify`.
      process.send(reply, Nil)
      actor.continue(handlers)
    }

    AddHandler(handler:) -> actor.continue(list.append(handlers, [handler]))

    CountHandlers(reply:) -> {
      process.send(reply, list.length(handlers))
      actor.continue(handlers)
    }
  }
}

/// Run one event past every handler, in order, and rebuild the list from
/// what they decided.
///
/// `list.filter_map` is exactly the shape of this: it visits head-first, so
/// handlers see the event in add order, and it preserves the order of what
/// it keeps, so a successor lands in its predecessor's position. A dropped
/// handler leaves no hole for its siblings to notice.
fn fan_out(
  handlers: List(Handler(event)),
  event: event,
) -> List(Handler(event)) {
  use handler <- list.filter_map(handlers)
  case handler.step(event) {
    Keep(handler:) -> Ok(handler)

    RemoveSelf -> Error(Nil)

    // A discard has to be visible. A handler that fails silently is a bus
    // that goes quiet, and the days spent finding out why are the reason
    // this line exists; `Failed` carries a reason precisely so there is
    // something to log.
    Failed(reason:) -> {
      sys.warn("weft/event_manager dropping a failed handler: " <> reason)
      Error(Nil)
    }
  }
}
