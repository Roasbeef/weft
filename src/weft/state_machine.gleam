//// A typed `gen_statem`: a state ADT, a data value, postponed events, and
//// the three timeout kinds, on a receive loop weft owns.
////
//// ## Why Gleam ships half of gen_statem's surface
////
//// Erlang's `gen_statem` has two callback modes. `state_functions`
//// dispatches on a state atom by naming a function per state;
//// `handle_event_function` hands every event to one callback and leaves the
//// dispatch to you. The split exists because a state is an atom and Erlang
//// cannot check that every state answered every event.
////
//// Gleam does not need it. A state ADT plus `case state, message` gives the
//// ergonomics of `state_functions` *and* exhaustiveness: add a variant and
//// every unhandled pair is a compile error. So this module ships the single
//// callback only, and one whole axis of gen_statem disappears with nothing
//// lost.
////
//// ## Why a loop of its own, rather than `weft/actor`
////
//// The two features that earn this module — postpone replay and the state
//// timeout — are not things a handler can do to itself. Replay has to put
//// events *ahead of the mailbox* in their original arrival order at the
//// moment the state changes, and a state timeout has to be cancelled by a
//// change of state the handler never mentions. Both are decisions the loop
//// makes between `receive` and the callback, so they belong to a loop, and
//// building them into `weft/actor` would mean the actor growing a state
//// concept it deliberately does not have. What is shared instead is the
//// machinery underneath: `weft/internal/sys` answers the debug plane and
//// `weft/internal/timer` keeps the timer book, exactly as they do for the
//// actor.
////
//// ## State and data are separate on purpose
////
//// The `state` a machine is *in* and the `data` it carries are separate
//// type parameters. The split is load-bearing rather than cosmetic: a state
//// timeout is cancelled by a change of `state` and is indifferent to
//// `data`, and postponed events are replayed by a change of `state` and
//// never by a change of `data`. Fold the two together and neither rule can
//// be stated.
////
//// ## The timeout taxonomy
////
//// Three kinds, distinguished only by what cancels them. All three are the
//// same mechanism underneath — a generation-stamped entry in the timer book
//// — so a fire that beat its own cancellation into the mailbox is
//// recognised and dropped rather than handled.
////
//// | Kind | Armed by | Cancelled by |
//// |---|---|---|
//// | State | `with_state_timeout` | a transition to a **different** state |
//// | Event | `with_event_timeout` | the **next event of any kind** |
//// | Named | `with_named_timeout` | `cancel_timeout` under the same name |
////
//// Each also dies by firing, since all three are one-shot.
////
//// The state timeout's rule is gen_statem's, down to the awkward corner:
//// `transition(to: s, data: d)` where `s` is the state the machine is
//// already in is **not** a state change. It cancels no state timeout, it
//// replays no postponed event, and it runs no enter callback. `keep` never
//// cancels a state timeout either. Only a move to a state that compares
//// unequal — structurally, by Gleam's `==` — counts.
////
//// The event timeout measures *quiet*, so anything that breaks the quiet
//// cancels it: a message from the mailbox, a message injected with
//// `then_handle`, a replayed postponed event, and any other timeout firing.
//// A handler that wants the deadline to continue must arm it again, which
//// is gen_statem's rule and not an oversight.
////
//// A named timeout survives everything until it fires or is cancelled by
//// name. It is the one for a deadline that belongs to a piece of work
//// rather than to a state.
////
//// ## Postpone, and the replay contract
////
//// `postpone` re-queues the event being handled. It is redelivered on the
//// next change of state, ahead of the mailbox, in the order the events
//// originally arrived, exactly once each — and an event may be postponed
//// again in the new state, as often as it takes. This is the action that
//// deletes hand-rolled pending queues, along with the ordering bug and the
//// "not ready yet" case arm that come with them.
////
//// The full ordering after a transition to a different state, which is
//// observable and which callers will depend on:
////
//// 1. The enter callback runs, immediately and in the calling process. It
////    is a callback, not a queued event.
//// 2. Messages the enter callback injected with `then_handle`, in the order
////    written.
//// 3. Messages the event handler injected with `then_handle`, in the order
////    written.
//// 4. Postponed events, in original arrival order.
//// 5. The mailbox.
////
//// Two and three are one rule rather than two: an injected block always
//// goes to the *front* of the pending work, depth-first, exactly as
//// `weft/actor`'s `then_handle` and gen_statem's `next_event` do. The enter
//// callback ran last, so its block is in front. A handler that injects on
//// every call never lets the queue drain and never reads its mailbox again;
//// that is a live-lock the type system cannot catch.
////
//// System messages are not starved by any of it. The debug plane is checked
//// between every two pending messages, so a `sys:suspend/1` arriving in the
//// middle of a replay takes effect there rather than after it drains.
////
//// ## Enter callbacks
////
//// `on_enter` runs on every transition where the state actually changed,
//// and once for the initial state — gen_statem makes the initial state
//// enter call too, and so do we. Since a same-state transition is not a
//// state change, `from == to` means exactly one thing: this is the initial
//// call. That is a useful thing for a callback to be able to test, and it
//// is the reason the initial call passes the state twice rather than
//// inventing an `Option`.
////
//// An enter callback may transition again, which runs another enter call,
//// and may stop the machine. It may not postpone, because there is no event
//// in hand to postpone: an enter call is caused by a state change, not by an
//// event.
////
//// That last rule is enforced by the type system rather than documented and
//// ignored at runtime. `Step` carries a fourth type parameter that says
//// whether the value came from a place where an event is in hand;
//// `Next` and `Enter` are the two aliases of it, `on_event` takes a handler
//// returning the first and `on_enter` a handler returning the second, and
//// `postpone` only accepts the first. Writing `postpone` in an enter
//// callback is a compile error naming `Postponable` and `Unpostponable`.
////
//// The alternative was a second family of constructors and actions for
//// enter callbacks — `enter_keep`, `enter_then_handle`, and so on — which
//// doubles the surface to be documented and learned for the sake of one
//// forbidden action. A marker parameter buys the same guarantee with one
//// set of functions, and the cost is confined to two type names a reader
//// meets in an error message they will only ever see once.
////
//// ## Example
////
//// The connection manager, which is the canonical gen_statem example
//// because every part of this module shows up in it:
////
//// ```gleam
//// pub type Link {
////   Connecting
////   Ready
////   Backoff
//// }
////
//// pub type Message {
////   Attempt
////   Established
////   Request(body: String)
//// }
////
//// fn handle(state: Link, data: Session, message: Message) {
////   case state, message {
////     // A request that arrives before the link is up is not an error and
////     // not a special case: it waits in the machine and is redelivered the
////     // instant we reach Ready, in the order it arrived.
////     Connecting, Request(..) | Backoff, Request(..) ->
////       sm.keep(data) |> sm.postpone
////
////     Connecting, Attempt ->
////       case dial(data) {
////         Ok(session) -> sm.transition(to: Ready, data: session)
////         // The retry is a state timeout, so it dies with the state: a
////         // stale Attempt can never arrive after something else has
////         // already moved us out of Backoff.
////         Error(_) ->
////           sm.transition(to: Backoff, data:)
////           |> sm.with_state_timeout(after: 1000, sending: Attempt)
////       }
////
////     Ready, Request(body:) -> sm.keep(send(data, body))
////     Backoff, Attempt -> sm.transition(to: Connecting, data:)
////     Connecting, Established | Ready, Established | Ready, Attempt |
////       Backoff, Established -> sm.keep(data)
////   }
//// }
////
//// // Entering Connecting starts the attempt, so every path into the state
//// // gets the effect — including the paths added next year.
//// fn entered(_from: Link, to: Link, data: Session) {
////   case to {
////     Connecting -> sm.keep(data) |> sm.then_handle(Attempt)
////     Ready | Backoff -> sm.keep(data)
////   }
//// }
////
//// pub fn start_link() {
////   sm.new(Connecting, new_session())
////   |> sm.on_event(handle)
////   |> sm.on_enter(entered)
////   |> sm.start
//// }
//// ```

import gleam/dict.{type Dict}
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

/// What the parent is given when a machine starts: the machine's pid and
/// whatever its initialiser chose to return.
///
/// This is `gleam/otp/actor`'s own type, not a copy of it, so a weft state
/// machine can be started by anything that expects a gleam_otp actor.
pub type Started(data) =
  otp_actor.Started(data)

/// The result of starting a state machine.
///
/// `gleam/otp/actor`'s own alias, for the same interoperability reason as
/// `Started`.
pub type StartResult(data) =
  otp_actor.StartResult(data)

/// Why a machine failed to start: the initialiser timed out, returned an
/// error, or the process died while running it.
///
/// `gleam/otp/actor`'s own type, so a supervisor cannot tell a weft state
/// machine from a gleam_otp actor.
pub type StartError =
  otp_actor.StartError

// ------------------------------------------------------------------- step

/// The marker on a step built where an event is in hand, and so may be
/// postponed. `Next` is `Step` carrying it.
///
/// It has no values. Its whole job is to be a name the compiler can refuse
/// to unify, and a name a reader can look up when it appears in the error
/// that refusal produces.
pub type Postponable

/// The marker on a step built where no event is in hand, and so may not be
/// postponed. `Enter` is `Step` carrying it.
///
/// An enter call is caused by a state change rather than by an event, so
/// there is nothing for `postpone` to re-queue. See this module's header for
/// why that is a type error rather than a runtime no-op.
pub type Unpostponable

/// What the machine does after a callback returns.
///
/// Built with `transition`, `keep`, `stop` or `stop_abnormal`, and refined
/// with `postpone`, the three `with_*_timeout` actions, `cancel_timeout` and
/// `then_handle`. Actions apply in the order they are written.
///
/// The fourth type parameter records where the step may be used. Callers
/// normally write `Next` or `Enter` rather than naming `Step` at all; it is
/// public because those two are aliases of it, and opaque because a step's
/// fields are the loop's business.
pub opaque type Step(state, data, message, postponing) {
  /// Carry on: stay put or move, having queued `injected` and the timeout
  /// changes in `timeouts`, and optionally re-queueing the event in hand.
  Advance(
    target: Target(state, data),
    postponed: Bool,
    injected: List(message),
    timeouts: List(TimeoutAction(message)),
    /// A replacement for the selector the loop receives with, from
    /// `with_selector`. `None` keeps the current one.
    selector: Option(Selector(message)),
  )

  /// Stop, exiting with this reason. Pending injected messages, postponed
  /// events and anything in the mailbox are discarded.
  Halt(reason: ExitReason)
}

/// What an event handler returns: a step that may postpone the event it was
/// given.
pub type Next(state, data, message) =
  Step(state, data, message, Postponable)

/// What an enter callback returns: a step with no event to postpone.
pub type Enter(state, data, message) =
  Step(state, data, message, Unpostponable)

/// Where a step leaves the machine.
///
/// `Moving` to the state the machine is already in is deliberately
/// representable: it is a legal thing to write, and it means what
/// gen_statem means by it, which is "no state change at all".
type Target(state, data) {
  Keeping(data: data)
  Moving(state: state, data: data)
}

/// One change to the timer book, applied in the order the caller wrote it.
type TimeoutAction(message) {
  Arm(key: TimerKey, after_ms: Int, message: message)
  Disarm(key: TimerKey)
}

/// The keys the machine arms timers under.
///
/// One ADT rather than three books, because the cancel-and-flush discipline
/// in `weft/internal/timer` is per key and the three kinds differ only in
/// what cancels them.
type TimerKey {
  StateTimeout
  EventTimeout
  NamedTimeout(name: String)
}

/// Move to `state`, carrying `data`.
///
/// Moving to a state that compares unequal to the current one is a state
/// change: it cancels the state timeout, replays postponed events ahead of
/// the mailbox, and runs the enter callback. Moving to the state the machine
/// is already in does none of those things, matching gen_statem — see this
/// module's header.
///
/// ## Examples
///
/// ```gleam
/// sm.transition(to: Ready, data: session)
/// // -> a state change, if the machine was not already Ready
/// ```
///
/// ```gleam
/// // The retry deadline is armed by the same step that enters Backoff, and
/// // survives it: the state timeout is cancelled before the step's own
/// // actions run.
/// sm.transition(to: Backoff, data:)
/// |> sm.with_state_timeout(after: 1000, sending: Attempt)
/// ```
pub fn transition(
  to state: state,
  data data: data,
) -> Step(state, data, message, postponing) {
  Advance(
    target: Moving(state:, data:),
    postponed: False,
    injected: [],
    timeouts: [],
    selector: None,
  )
}

/// Stay in the current state with new data.
///
/// Nothing is cancelled and nothing is replayed: `keep` is not a state
/// change however much the data moved. A state timeout armed earlier keeps
/// running.
///
/// ## Examples
///
/// ```gleam
/// sm.keep(Session(..data, sent: data.sent + 1))
/// // -> same state, new data, state timeout still ticking
/// ```
pub fn keep(data: data) -> Step(state, data, message, postponing) {
  Advance(
    target: Keeping(data:),
    postponed: False,
    injected: [],
    timeouts: [],
    selector: None,
  )
}

/// Stop and shut down, handling no further events.
///
/// The exit reason is `Normal`, so a `Permanent` child restarts and a
/// `Transient` one does not.
///
/// ## Examples
///
/// ```gleam
/// case message {
///   Shutdown -> sm.stop()
///   Request(body:) -> sm.keep(send(data, body))
/// }
/// ```
pub fn stop() -> Step(state, data, message, postponing) {
  Halt(Normal)
}

/// Stop and shut down abnormally, propagating the reason to linked
/// processes.
///
/// ## Examples
///
/// ```gleam
/// sm.stop_abnormal("the peer sent a frame we cannot parse")
/// // -> linked processes are told; a Permanent child is restarted
/// ```
pub fn stop_abnormal(reason: String) -> Step(state, data, message, postponing) {
  Halt(Abnormal(dynamic.string(reason)))
}

/// Re-queue the event being handled, to be redelivered on the next change of
/// state.
///
/// This is the action that deletes hand-rolled pending queues. Postponed
/// events are replayed ahead of the mailbox, in the order they originally
/// arrived, exactly once each; an event may be postponed again in the new
/// state, and takes its place at the back of the queue as it does. Postponing
/// on a step that is *itself* a state change is meaningful and useful: the
/// event is redelivered immediately, in the state being moved to.
///
/// The type refuses this in an enter callback, which has no event in hand.
/// Applying it to a `stop` value does nothing: a machine that is stopping
/// redelivers nothing.
///
/// Nothing bounds the postponed queue but the machine's own logic. A state
/// that postpones an event it can never leave will hold it forever, which is
/// the one way this action is worse than the pending list it replaces —
/// there the leak is at least visible.
///
/// ## Examples
///
/// ```gleam
/// // A request that arrives before the link is up waits in the machine.
/// Connecting, Request(..) -> sm.keep(data) |> sm.postpone
/// ```
pub fn postpone(
  next: Next(state, data, message),
) -> Next(state, data, message) {
  case next {
    Advance(target:, postponed: _, injected:, timeouts:, selector:) ->
      Advance(target:, postponed: True, injected:, timeouts:, selector:)
    Halt(reason:) -> Halt(reason:)
  }
}

/// Send the machine `message` after `ms` milliseconds, unless it leaves this
/// state first.
///
/// The deadline belongs to the state, which is what makes it safe: a fire
/// that raced the transition out of the state is recognised as stale by the
/// timer book and dropped, so a retry can never be handled in a state that
/// did not ask for it. A transition to the *same* state does not cancel it,
/// and neither does `keep`.
///
/// Arming replaces any state timeout already armed. Applying this to a
/// `stop` value does nothing.
///
/// ## Examples
///
/// ```gleam
/// sm.transition(to: Backoff, data:)
/// |> sm.with_state_timeout(after: 1000, sending: Attempt)
/// // -> Attempt in a second, unless Backoff is left first
/// ```
pub fn with_state_timeout(
  step: Step(state, data, message, postponing),
  after ms: Int,
  sending message: message,
) -> Step(state, data, message, postponing) {
  timing(step, Arm(key: StateTimeout, after_ms: ms, message:))
}

/// Send the machine `message` after `ms` milliseconds with no events at all.
///
/// This one measures quiet, so the next event of any kind cancels it —
/// including a message injected with `then_handle`, a replayed postponed
/// event, and another timeout firing. A handler that wants the deadline to
/// continue arms it again, which is gen_statem's rule.
///
/// Arming replaces any event timeout already armed. Applying this to a
/// `stop` value does nothing.
///
/// ## Examples
///
/// ```gleam
/// // Give up on a half-open connection that has gone quiet.
/// sm.keep(data) |> sm.with_event_timeout(after: 30_000, sending: PeerSilent)
/// ```
pub fn with_event_timeout(
  step: Step(state, data, message, postponing),
  after ms: Int,
  sending message: message,
) -> Step(state, data, message, postponing) {
  timing(step, Arm(key: EventTimeout, after_ms: ms, message:))
}

/// Send the machine `message` after `ms` milliseconds, whatever else
/// happens.
///
/// A named timeout survives state changes and events alike; only firing or
/// `cancel_timeout` under the same name ends it. It is the timeout for a
/// deadline that belongs to a piece of work rather than to a state.
///
/// Arming replaces any timeout already armed under the same name. Applying
/// this to a `stop` value does nothing.
///
/// ## Examples
///
/// ```gleam
/// sm.keep(data)
/// |> sm.with_named_timeout(name: "handshake", after: 5000, sending: TooSlow)
/// // -> TooSlow in five seconds, in whatever state the machine has reached
/// ```
pub fn with_named_timeout(
  step: Step(state, data, message, postponing),
  name name: String,
  after ms: Int,
  sending message: message,
) -> Step(state, data, message, postponing) {
  timing(step, Arm(key: NamedTimeout(name:), after_ms: ms, message:))
}

/// Cancel the named timeout armed under `name`.
///
/// Cancelling a name nothing is armed under is a no-op, which is what lets a
/// handler cancel unconditionally rather than tracking whether it armed
/// anything. A fire already in flight when this runs is dropped by the timer
/// book rather than handled.
///
/// Applying this to a `stop` value does nothing.
///
/// ## Examples
///
/// ```gleam
/// sm.transition(to: Ready, data:) |> sm.cancel_timeout(name: "handshake")
/// // -> the handshake deadline is gone, even if it just fired
/// ```
pub fn cancel_timeout(
  step: Step(state, data, message, postponing),
  name name: String,
) -> Step(state, data, message, postponing) {
  timing(step, Disarm(NamedTimeout(name:)))
}

/// Add one timer-book change to a step, keeping the order it was written in.
///
/// The list is the handful of actions one callback chose, so appending to
/// keep call-site order costs nothing worth saving.
fn timing(
  step: Step(state, data, message, postponing),
  action: TimeoutAction(message),
) -> Step(state, data, message, postponing) {
  case step {
    Advance(target:, postponed:, injected:, timeouts:, selector:) ->
      Advance(
        target:,
        postponed:,
        injected:,
        timeouts: list.append(timeouts, [action]),
        selector:,
      )
    Halt(reason:) -> Halt(reason:)
  }
}

/// Replace the selector the machine receives with going forward.
///
/// This replaces the selector given at initialisation or by an earlier
/// step rather than adding to it, so a selector that no longer selects the
/// machine's own subject stops receiving on it. It is the way a machine
/// widens its mailbox to a channel that did not exist when it started — a
/// subject the handler itself just created, a monitor it just installed —
/// which otherwise forces the whole first phase into the initialiser.
/// Applying this to `stop` does nothing: there is no "going forward".
///
/// ## Examples
///
/// ```gleam
/// let inner = process.new_subject()
/// sm.transition(to: Forwarding, data: Request(..data, inner:))
/// |> sm.with_selector(
///   process.new_selector()
///   |> process.select(control)
///   |> process.select_map(inner, InnerEvent),
/// )
/// ```
pub fn with_selector(
  step: Step(state, data, message, postponing),
  selector: Selector(message),
) -> Step(state, data, message, postponing) {
  case step {
    Advance(target:, postponed:, injected:, timeouts:, selector: _) ->
      Advance(
        target:,
        postponed:,
        injected:,
        timeouts:,
        selector: Some(selector),
      )
    Halt(reason:) -> Halt(reason:)
  }
}

/// Handle `message` next, ahead of the mailbox.
///
/// This is gen_statem's `next_event`: the supported way for a machine to
/// drive itself, and the reason an enter callback can start the work its
/// state exists to do. The injected message goes through the ordinary event
/// handler in the ordinary way, and — like any event — cancels the event
/// timeout.
///
/// Ordering is the contract in this module's header. Messages injected by
/// one callback run in the order they were written, and the whole block runs
/// in front of anything queued earlier, depth-first. A callback that injects
/// on every call never lets the queue drain and never reads its mailbox
/// again.
///
/// Applying this to a `stop` value does nothing: a machine that is stopping
/// handles no more events.
///
/// ## Examples
///
/// ```gleam
/// // Every path into Connecting starts the attempt, including the ones
/// // added later.
/// Connecting -> sm.keep(data) |> sm.then_handle(Attempt)
/// ```
///
/// ```gleam
/// // `Flush` is handled first, then `Compact`, then anything else pending.
/// sm.keep(data) |> sm.then_handle(Flush) |> sm.then_handle(Compact)
/// ```
pub fn then_handle(
  step: Step(state, data, message, postponing),
  message: message,
) -> Step(state, data, message, postponing) {
  case step {
    Advance(target:, postponed:, injected:, timeouts:, selector:) ->
      Advance(
        target:,
        postponed:,
        injected: list.append(injected, [message]),
        timeouts:,
        selector:,
      )
    Halt(reason:) -> Halt(reason:)
  }
}

// ------------------------------------------------------------ initialised

/// The outcome of a machine's initialiser: the starting state and data, the
/// selector to receive with, the value to hand back to the parent, and any
/// events to handle before the mailbox.
///
/// Built with `initialised` and refined with `selecting`, `returning` and
/// `continuing`.
pub opaque type Initialised(state, data, message, return) {
  Initialised(
    state: state,
    data: data,
    selector: Option(Selector(message)),
    return: return,
    injected: List(message),
  )
}

/// Take the post-initialisation state and data of the machine.
///
/// ## Examples
///
/// ```gleam
/// sm.new_with_initialiser(1000, fn(subject) {
///   sm.initialised(Connecting, new_session()) |> sm.returning(subject) |> Ok
/// })
/// ```
pub fn initialised(
  state: state,
  data: data,
) -> Initialised(state, data, message, Nil) {
  Initialised(state:, data:, selector: None, return: Nil, injected: [])
}

/// Give the machine a selector to receive messages with.
///
/// This replaces the default selector, which selects only the machine's own
/// subject, so a custom selector must select that subject itself if the
/// machine is still to receive on it. A message that arrives and is not
/// selected for is discarded with a warning.
///
/// The message type is fixed rather than changed by this call, for the same
/// reason as in `weft/actor`: the injected queue mentions the message type
/// too, so a type-changing `selecting` would have to silently discard
/// anything `continuing` had already added.
///
/// ## Examples
///
/// ```gleam
/// sm.new_with_initialiser(1000, fn(subject) {
///   let selector =
///     process.new_selector()
///     |> process.select(subject)
///     |> process.select_map(peer_events, FromPeer)
///   sm.initialised(Connecting, session)
///   |> sm.selecting(selector)
///   |> sm.returning(subject)
///   |> Ok
/// })
/// ```
pub fn selecting(
  initialised: Initialised(state, data, message, return),
  selector: Selector(message),
) -> Initialised(state, data, message, return) {
  Initialised(..initialised, selector: Some(selector))
}

/// Set the value handed back to the parent when the machine has started.
///
/// Commonly the subject the machine receives on, so the parent can send to
/// it.
///
/// ## Examples
///
/// ```gleam
/// sm.new_with_initialiser(1000, fn(subject) {
///   sm.initialised(Connecting, session) |> sm.returning(subject) |> Ok
/// })
/// ```
pub fn returning(
  initialised: Initialised(state, data, message, old_return),
  return: return,
) -> Initialised(state, data, message, return) {
  Initialised(
    state: initialised.state,
    data: initialised.data,
    selector: initialised.selector,
    return:,
    injected: initialised.injected,
  )
}

/// Handle `message` before anything in the mailbox.
///
/// This is gen_server's `handle_continue` and gen_statem's initial
/// `next_event` in one: the initialiser returns at once, so `start`
/// unblocks and the supervisor moves on, and the message is still handled
/// before the first external one. The guarantee is not statistical — the
/// queue is filled before the acknowledgement that releases `start`, and the
/// loop never looks at the mailbox while the queue is non-empty.
///
/// The initial enter callback runs before these, and anything it injects
/// goes in front of them, by the depth-first rule in this module's header.
///
/// ## Examples
///
/// ```gleam
/// sm.new_with_initialiser(1000, fn(subject) {
///   sm.initialised(Connecting, session)
///   |> sm.returning(subject)
///   |> sm.continuing(Attempt)
///   |> Ok
/// })
/// ```
pub fn continuing(
  initialised: Initialised(state, data, message, return),
  message: message,
) -> Initialised(state, data, message, return) {
  Initialised(
    ..initialised,
    injected: list.append(initialised.injected, [
      message,
    ]),
  )
}

// ---------------------------------------------------------------- builder

/// A description of a state machine, ready to `start` or to hand to a
/// supervisor.
///
/// Built with `new` or `new_with_initialiser` and refined with the setters
/// below.
pub opaque type Builder(state, data, message, return) {
  Builder(
    /// Run in the new process before the parent is told the machine
    /// started. An error here fails the start and is reported to the parent.
    initialise: fn(Subject(message)) ->
      Result(Initialised(state, data, message, return), String),
    /// How long the initialiser has, in milliseconds, before the machine is
    /// killed and the start reported as `InitTimeout`.
    initialisation_timeout: Int,
    /// Called for every event: mailbox messages, injected messages,
    /// replayed postponed events and timeouts alike.
    on_event: fn(state, data, message) -> Next(state, data, message),
    /// Called on every real state change, and once for the initial state.
    on_enter: Option(fn(state, state, data) -> Enter(state, data, message)),
    /// The name to register the machine under, if any.
    name: Option(process.Name(message)),
    /// Whether the machine traps exits, turning exit signals from linked
    /// processes into messages the loop can act on.
    trap_exits: Bool,
    /// Whether `start` links the new process to its starter.
    linkage: Linkage,
  )
}

/// Describe a machine starting in `state` with `data`, and no custom
/// initialisation.
///
/// The machine hands the parent a subject to send messages on — a named
/// subject if `named` was used.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(started) =
///   sm.new(Connecting, new_session())
///   |> sm.on_event(handle)
///   |> sm.start
/// sm.send(started.data, Request("GET /"))
/// ```
pub fn new(
  state: state,
  data: data,
) -> Builder(state, data, message, Subject(message)) {
  let initialise = fn(subject) {
    initialised(state, data) |> returning(subject) |> Ok
  }
  Builder(
    initialise:,
    initialisation_timeout: 1000,
    on_event: fn(_state, data, _message) { keep(data) },
    on_enter: None,
    name: None,
    trap_exits: False,
    linkage: Linked,
  )
}

/// Describe a machine with initialisation that runs in the new process
/// before `start` returns.
///
/// `timeout` is how many milliseconds the initialiser has; overrunning it
/// kills the machine and fails the start with `InitTimeout`. The machine's
/// default subject is passed in — return it to the parent with `returning`,
/// use it some other way, or ignore it.
///
/// Work that does not have to happen before `start` returns should not:
/// `continuing`, or an enter callback on the initial state, runs it after
/// the acknowledgement and still before the first external message.
///
/// ## Examples
///
/// ```gleam
/// sm.new_with_initialiser(1000, fn(subject) {
///   use socket <- result.try(open())
///   sm.initialised(Connecting, Session(socket:))
///   |> sm.returning(subject)
///   |> Ok
/// })
/// |> sm.on_event(handle)
/// |> sm.start
/// ```
pub fn new_with_initialiser(
  timeout: Int,
  initialise: fn(Subject(message)) ->
    Result(Initialised(state, data, message, return), String),
) -> Builder(state, data, message, return) {
  Builder(
    initialise:,
    initialisation_timeout: timeout,
    on_event: fn(_state, data, _message) { keep(data) },
    on_enter: None,
    name: None,
    trap_exits: False,
    linkage: Linked,
  )
}

/// Set the event handler.
///
/// It is called with the current state, the current data and one event, and
/// returns a `Next`. Writing it as `case state, message` is the point of the
/// module: add a state variant and the compiler names every pair that has
/// not been thought about.
///
/// ## Examples
///
/// ```gleam
/// sm.new(Connecting, session)
/// |> sm.on_event(fn(state, data, message) {
///   case state, message {
///     Connecting, Request(..) -> sm.keep(data) |> sm.postpone
///     Connecting, Established -> sm.transition(to: Ready, data:)
///     Ready, Request(body:) -> sm.keep(send(data, body))
///     Ready, Established -> sm.keep(data)
///   }
/// })
/// ```
pub fn on_event(
  builder: Builder(state, data, message, return),
  handler: fn(state, data, message) -> Next(state, data, message),
) -> Builder(state, data, message, return) {
  Builder(..builder, on_event: handler)
}

/// Set the enter callback, run on every real state change and once for the
/// initial state.
///
/// The arguments are the state left, the state entered, and the data as the
/// transition left it. On the initial call the two states are the same value,
/// which cannot happen otherwise: a same-state transition is not a state
/// change and runs no enter call at all.
///
/// The callback returns an `Enter`, which is a `Next` without `postpone` —
/// there is no event in hand to re-queue. It may transition again, which
/// runs another enter call, and it may stop the machine.
///
/// ## Examples
///
/// ```gleam
/// // Every path into Connecting starts an attempt; entering Backoff arms
/// // the retry that will leave it.
/// sm.new(Connecting, session)
/// |> sm.on_enter(fn(_from, to, data) {
///   case to {
///     Connecting -> sm.keep(data) |> sm.then_handle(Attempt)
///     Backoff ->
///       sm.keep(data) |> sm.with_state_timeout(after: 1000, sending: Attempt)
///     Ready -> sm.keep(data)
///   }
/// })
/// ```
pub fn on_enter(
  builder: Builder(state, data, message, return),
  handler: fn(state, state, data) -> Enter(state, data, message),
) -> Builder(state, data, message, return) {
  Builder(..builder, on_enter: Some(handler))
}

/// Register the machine under `name` when it starts, so it can be reached by
/// a named subject rather than by passing a subject around.
///
/// If the name is already registered the machine fails to start. When this
/// is used the machine's default subject is the named one, which is what
/// lets a restarted machine take over from the one it replaced.
///
/// ## Examples
///
/// ```gleam
/// let name = process.new_name("link")
/// let assert Ok(_) =
///   sm.new(Connecting, session) |> sm.named(name) |> sm.start
/// sm.send(process.named_subject(name), Request("GET /"))
/// ```
pub fn named(
  builder: Builder(state, data, message, return),
  name: process.Name(message),
) -> Builder(state, data, message, return) {
  Builder(..builder, name: Some(name))
}

/// Whether `start` links the new process to the process that started it.
pub type Linkage {
  /// The default, and what OTP does: the starter and the machine share
  /// fate down a link, and a supervisor is that starter.
  Linked

  /// No link. The machine is started by a process that must neither die
  /// with it nor take it down: a guard started from the consumer it
  /// serves, a holder that must outlive the host that created it. The
  /// starter still learns of a start failure through the acknowledgement,
  /// and can monitor the pid it gets back for everything after.
  Unlinked
}

/// Start the machine without linking it to its starter.
///
/// Consumers that need this otherwise pay for it with a throwaway
/// process that starts the machine and exits, which leaves the machine
/// linked to a corpse; this is that arrangement made a setting. Only
/// `start` reads it — a supervisor always links its children, so
/// `supervised` ignores it.
///
/// ## Examples
///
/// ```gleam
/// builder |> sm.unlinked |> sm.start
/// ```
pub fn unlinked(
  builder: Builder(state, data, message, return),
) -> Builder(state, data, message, return) {
  Builder(..builder, linkage: Unlinked)
}

/// Choose whether the machine traps exits.
///
/// A trapping machine receives an exit signal from a linked process as a
/// message instead of dying of it. The policy the loop then applies mirrors
/// what would have happened without trapping: an exit from the parent shuts
/// the machine down whatever the reason — including `Normal`, as OTP
/// behaviours do, because a child outliving its parent is a leak — and an
/// exit from any other linked process shuts it down only if the reason is
/// abnormal.
///
/// The one thing trapping buys here is promptness rather than cleanup: a
/// *suspended* machine still watches for exits, so a supervisor terminating
/// one does not have to wait out the whole shutdown timeout and then kill
/// it.
///
/// Trapping is off by default, and turning it on changes what kills the
/// machine: `process.kill` still does, `process.send_exit` no longer does.
///
/// ## Examples
///
/// ```gleam
/// sm.new(Connecting, session) |> sm.trapping_exits(True)
/// ```
pub fn trapping_exits(
  builder: Builder(state, data, message, return),
  trap: Bool,
) -> Builder(state, data, message, return) {
  Builder(..builder, trap_exits: trap)
}

// ------------------------------------------------------------------ start

/// Start a state machine from a builder.
///
/// The new process is linked to the caller, and the caller blocks until the
/// initialiser has finished or the initialisation timeout expires. The
/// initial enter callback and any messages added with `continuing` are
/// handled after this returns and before any message sent afterwards.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(started) =
///   sm.new(Connecting, session) |> sm.on_event(handle) |> sm.start
/// // -> `started.data` is a subject; `started.pid` is the machine
/// ```
pub fn start(
  builder: Builder(state, data, message, return),
) -> Result(Started(return), StartError) {
  let timeout = builder.initialisation_timeout
  let ack_subject = process.new_subject()
  let parent = process.self()

  let child = case builder.linkage {
    Linked ->
      process.spawn(fn() { initialise_machine(builder, parent, ack_subject) })
    Unlinked ->
      process.spawn_unlinked(fn() {
        initialise_machine(builder, parent, ack_subject)
      })
  }

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
  // surprise `Down` to the parent if the machine died later.
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

/// Describe this machine as a supervisor's child.
///
/// Returns `gleam/otp/supervision`'s own `ChildSpecification`, so a weft
/// state machine is added to a `gleam_otp` supervisor exactly as an upstream
/// actor is. The default is a permanent worker with a five-second shutdown;
/// refine it with `supervision.restart`, `supervision.timeout` and friends.
///
/// ## Examples
///
/// ```gleam
/// supervisor.new(supervisor.OneForOne)
/// |> supervisor.add(sm.supervised(link_builder))
/// |> supervisor.start
/// ```
pub fn supervised(
  builder: Builder(state, data, message, return),
) -> supervision.ChildSpecification(return) {
  supervision.worker(fn() { start(builder) })
}

/// Send an event to a state machine.
///
/// A re-export of `process.send`, for convenience.
///
/// ## Examples
///
/// ```gleam
/// sm.send(started.data, Request("GET /"))
/// ```
pub fn send(subject: Subject(message), message: message) -> Nil {
  process.send(subject, message)
}

/// Send an event and wait for the reply.
///
/// The caller crashes if no reply arrives within the timeout, rather than
/// carrying on against a machine that may be in an unknown state. A
/// re-export of `process.call`.
///
/// Beware of calling a machine that might postpone the call: the reply
/// subject waits in the postponed queue until the state changes, and if it
/// never does, the caller crashes on the timeout.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(body) = sm.call(subject, waiting: 1000, sending: Fetch)
/// ```
pub fn call(
  subject: Subject(message),
  waiting timeout: Int,
  sending make_message: fn(Subject(reply)) -> message,
) -> reply {
  process.call(subject, timeout, make_message)
}

// ------------------------------------------------------------------- loop

/// Everything the receive loop carries between events.
type Self(state, data, message) {
  Self(
    /// The OTP debug plane: parent, mode, and the answers to `sys`.
    plane: sys.Plane,
    /// The state the machine is in. Compared structurally to decide whether
    /// a step is a state change.
    state: state,
    /// The data the machine carries across transitions.
    data: data,
    /// The programmer's selector, already wrapped into `Event`.
    selector: Selector(Event(message)),
    /// Events awaiting handling, in the order they run: injected messages
    /// and replayed postponed events. While this is non-empty the mailbox is
    /// not consulted.
    queue: List(message),
    /// Postponed events, **newest first**. Kept reversed so that postponing
    /// is a cons rather than an append; `list.reverse` restores arrival
    /// order once, at replay.
    postponed: List(message),
    /// The programmer's event handler.
    on_event: fn(state, data, message) -> Next(state, data, message),
    /// The programmer's enter callback, if configured.
    on_enter: Option(fn(state, state, data) -> Enter(state, data, message)),
    /// Whether exit signals arrive as messages. Decides whether the loop
    /// installs the trapped-exit arm at all.
    trapping: Bool,
    /// The timer book: the three timeout kinds and the flush that makes a
    /// cancelled one safe.
    timers: timer.Timers(TimerKey, message),
    /// What each live timer was armed *with*. The book is opaque and cannot
    /// be asked, and a suspension has to be able to disarm every timer and
    /// then put them all back — see `handle_system`.
    arming: Dict(TimerKey, Arming(message)),
  )
}

/// The arming of one timer, remembered so that a resume can reinstate it.
type Arming(message) {
  Arming(after_ms: Int, message: message)
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
  /// the machine traps exits.
  Trapped(exit: process.ExitMessage)

  /// A timeout fired. It may be stale; the timer book decides.
  Fired(fired: timer.Fired(TimerKey, message))

  /// A message no selector arm claimed. Discarded with a warning.
  Unexpected(message: Dynamic)
}

/// Run the machine's initialisation in the newly spawned process and, if it
/// succeeds, hand over to the loop.
///
/// The order here is the whole `continuing` guarantee, as it is in
/// `weft/actor`: the queue is populated from the `Initialised` value before
/// the acknowledgement is sent, and the loop drains the queue before it
/// looks at the mailbox. So however the parent races — handing the subject
/// to a client the instant `start` returns — no external message can be
/// handled first.
///
/// The initial enter call happens on the far side of the acknowledgement,
/// for the same reason: it may do real work, and a supervisor's start
/// timeout should not be paying for it.
fn initialise_machine(
  builder: Builder(state, data, message, return),
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

    Ok(#(subject, Initialised(state:, data:, selector:, return:, injected:))) -> {
      let selector = case selector {
        Some(selector) -> selector
        None -> process.new_selector() |> process.select(subject)
      }

      let self =
        Self(
          plane: sys.new(module: "weft@state_machine", parent:),
          state:,
          data:,
          selector: process.map_selector(selector, Received),
          queue: injected,
          postponed: [],
          on_event: builder.on_event,
          on_enter: builder.on_enter,
          trapping: builder.trap_exits,
          timers: timer.new(process.new_subject()),
          arming: dict.new(),
        )

      // The queue is already loaded, so releasing the parent here cannot let
      // an external message overtake a continue or the initial enter call.
      process.send(ack, Ok(return))

      // gen_statem makes a state enter call for the initial state too, with
      // the old state equal to the new one. Passing the state twice is what
      // makes `from == to` mean "this is the initial call" and nothing else.
      enter(self, from: state, to: state)
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

/// What a suspended machine is still willing to receive.
///
/// Two things, and no more. The debug plane, because `resume` arrives on it
/// and a suspension nothing can lift is a hang. And, for a machine that
/// traps exits, an exit signal — because a supervisor shutting down a
/// suspended child would otherwise wait out its whole shutdown timeout and
/// then kill it. OTP's own `sys:suspend_loop` makes the same exception for
/// the same reason.
type Frozen {
  FrozenSystem(incoming: sys.Incoming)
  FrozenExit(exit: process.ExitMessage)
}

/// The receive loop.
///
/// Suspension is checked first because it overrides everything else: a
/// suspended machine may not handle mailbox messages, may not drain its
/// queue of injected and replayed events, and may not time out.
/// `sys:suspend/1` promises its caller a frozen process, and the only way to
/// keep that promise is to serve nothing but the debug plane — and its own
/// death — until `resume` arrives.
fn loop(self: Self(state, data, message)) -> ExitReason {
  case sys.is_suspended(self.plane) {
    True ->
      case process.selector_receive_forever(frozen_selector(self.trapping)) {
        FrozenSystem(incoming:) -> loop(handle_system(self, incoming))
        FrozenExit(exit:) -> handle_exit(self, exit)
      }
    False -> run(self)
  }
}

/// The selector for a suspended machine.
fn frozen_selector(trapping: Bool) -> Selector(Frozen) {
  let selector = process.new_selector() |> sys.selecting(FrozenSystem)
  case trapping {
    False -> selector
    True -> process.select_trapped_exits(selector, FrozenExit)
  }
}

/// The running half of the loop: pending work first, then the mailbox.
///
/// The queue holds both injected messages and replayed postponed events, in
/// the order this module's header fixes. Nothing in the mailbox is looked at
/// while it is non-empty, which is what makes "ahead of the mailbox" a
/// guarantee rather than a race the machine usually wins.
fn run(self: Self(state, data, message)) -> ExitReason {
  case self.queue {
    [] ->
      dispatch(self, process.selector_receive_forever(running_selector(self)))

    [message, ..rest] ->
      // The debug plane is checked between every two pending events, so a
      // `suspend` arriving mid-replay takes effect here rather than after
      // the replay drains. Handling one and going back through `loop`
      // rather than draining them all is deliberate: the mode change has to
      // be observed before the next event runs.
      case poll_system(self) {
        Some(self) -> loop(self)
        None -> handle(Self(..self, queue: rest), message)
      }
  }
}

/// Take one already-arrived system message without blocking, if there is
/// one.
///
/// The scan is over the whole mailbox, as any selective receive is, and it
/// happens once per pending event; a long replay in front of a deep mailbox
/// pays for that. The alternative — draining the queue first and reading
/// system messages after — would let a machine that always has pending work
/// starve the debug plane, which is the failure this ordering exists to
/// prevent.
fn poll_system(
  self: Self(state, data, message),
) -> Option(Self(state, data, message)) {
  case process.selector_receive(system_selector(), 0) {
    Error(Nil) -> None
    Ok(incoming) -> Some(handle_system(self, incoming))
  }
}

/// The selector for the mid-replay poll: the debug plane and nothing else.
fn system_selector() -> Selector(sys.Incoming) {
  process.new_selector() |> sys.selecting(fn(incoming) { incoming })
}

/// The selector for a running machine.
///
/// The arms weft owns go on first so that a programmer's selector can
/// deliberately replace them. The system arm goes on last, where nothing can
/// shadow it: a machine invisible to `sys` is a debugging dead end, and that
/// is not a choice worth offering. The timer arm maps to `Fired` rather than
/// straight to the handler, because every fire must go through
/// `timer.accept` — routing one around it puts the stale-fire bug back.
fn running_selector(
  self: Self(state, data, message),
) -> Selector(Event(message)) {
  process.new_selector()
  |> process.select_other(Unexpected)
  |> select_exits(self.trapping)
  |> process.select_map(timer.subject(self.timers), Fired)
  |> process.merge_selector(self.selector)
  |> sys.selecting(System)
}

/// Add the trapped-exit arm, but only for a machine that traps: without
/// trapping no such message can arrive, and installing the arm would suggest
/// otherwise.
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
fn dispatch(
  self: Self(state, data, message),
  event: Event(message),
) -> ExitReason {
  case event {
    System(incoming:) -> loop(handle_system(self, incoming))

    Trapped(exit:) -> handle_exit(self, exit)

    Fired(fired:) -> handle_fired(self, fired)

    // Not selected for by anyone, so the programmer has not accounted for
    // it. Discarding it silently would make a mis-wired selector look like a
    // hung machine.
    Unexpected(message:) -> {
      sys.warn(
        "weft/state_machine discarding unexpected message: "
        <> string.inspect(message),
      )
      loop(self)
    }

    Received(message:) -> handle(self, message)
  }
}

/// Answer the debug plane, and keep the timers consistent with the mode.
///
/// A suspended machine is frozen, not merely quiet, so every timer is
/// disarmed on the way in and re-armed on the way out. Leaving them armed
/// would fire a timeout describing a stretch the machine was not allowed to
/// act in, and would break the promise `sys:suspend/1` makes to its caller.
///
/// This is a deliberate departure from Erlang's gen_statem, whose timers
/// keep running through a suspension and whose timeout events pile up in the
/// mailbox to be handled all at once on resume. Re-arming from full is the
/// honest reading of "frozen": the deadline measures time the machine could
/// have acted in, and none of the suspension was.
fn handle_system(
  self: Self(state, data, message),
  incoming: sys.Incoming,
) -> Self(state, data, message) {
  case incoming {
    sys.Unimplemented(request:) -> {
      sys.warn(
        "weft/state_machine received an unimplemented system message: "
        <> string.inspect(request),
      )
      self
    }

    sys.Request(message:) -> {
      let was_suspended = sys.is_suspended(self.plane)

      // The state handed out is the pair, because a machine's state is the
      // pair: `#(state, data)` is what a reader of `sys:get_state/1` or the
      // observer needs to make sense of what it is looking at.
      let plane =
        sys.handle(self.plane, message, holding: #(self.state, self.data))
      let self = Self(..self, plane:)

      case was_suspended, sys.is_suspended(plane) {
        False, True -> Self(..self, timers: timer.cancel_all(self.timers))
        True, False -> rearm_all(self)
        False, False -> self
        True, True -> self
      }
    }
  }
}

/// Put back every timer the suspension took down.
///
/// The arming record survived the freeze precisely so that this can happen;
/// each timer restarts from its full duration and takes a fresh generation
/// stamp, so a fire that was already in the mailbox when the suspension
/// disarmed it is recognised as stale and dropped rather than delivered on
/// resume.
fn rearm_all(self: Self(state, data, message)) -> Self(state, data, message) {
  use self, key, arming <- dict.fold(self.arming, self)
  arm(self, key, arming.after_ms, arming.message)
}

/// Decide what a trapped exit signal means.
///
/// The parent's exit takes the machine with it whatever the reason, which is
/// what every OTP behaviour does: a child that outlives the process that
/// started it is a leak, and `Normal` is not an exception to that. Any other
/// linked process is treated as the link itself would have treated it —
/// ignored if it exited normally, fatal otherwise — so that turning trapping
/// on does not quietly change the topology.
fn handle_exit(
  self: Self(state, data, message),
  exit: process.ExitMessage,
) -> ExitReason {
  case exit.pid == sys.parent(self.plane) {
    True -> exit_process(exit.reason)

    False ->
      case exit.reason {
        Normal -> loop(self)
        Killed -> exit_process(Killed)
        Abnormal(reason:) -> exit_process(Abnormal(reason:))
      }
  }
}

/// Deliver a timer message, or drop it.
///
/// This is the consumer half of the timer book's cancel-with-flush, and the
/// only route a `Fired` message may take into the machine: a fire whose
/// arming was cancelled or replaced dies here and never reaches the event
/// handler. That is what lets a state timeout promise it can never be
/// handled in a state that did not arm it.
fn handle_fired(
  self: Self(state, data, message),
  fired: timer.Fired(TimerKey, message),
) -> ExitReason {
  case timer.accept(self.timers, fired) {
    timer.Stale(timers:) -> loop(Self(..self, timers:))

    timer.Deliver(timers:, key:, message:) -> {
      // These are one-shot timers, so the arming record has to follow the
      // book's own entry out; leaving it would have a resume re-arm a
      // timeout that has already fired.
      let arming = dict.delete(self.arming, key)
      handle(Self(..self, timers:, arming:), message)
    }
  }
}

/// Run the event handler for one event.
///
/// The event timeout is cancelled *before* the handler runs, not after: it
/// measures quiet, this event ends the quiet, and a handler that arms a new
/// one must not have it cancelled again on the way out.
fn handle(self: Self(state, data, message), message: message) -> ExitReason {
  let self = disarm(self, EventTimeout)
  commit(self, self.on_event(self.state, self.data, message), [message])
}

/// Interpret a step: the one place a `Next` or an `Enter` becomes state
/// changes, timers and queued work.
///
/// `current` is the event in hand, as a list rather than an option, because
/// an enter call has no event at all and an empty list says so without a
/// `case` arm that cannot happen. It holds one element at most, so the
/// prepend in `hold` is constant time.
fn commit(
  self: Self(state, data, message),
  step: Step(state, data, message, postponing),
  current: List(message),
) -> ExitReason {
  case step {
    Halt(reason:) -> exit_process(reason)

    Advance(target:, postponed:, injected:, timeouts:, selector:) -> {
      let self = hold(self, postponed, current)
      let from = self.state
      let self = retarget(self, target)

      // A replaced selector takes effect from the next receive, which is
      // what lets a handler open a channel it could not have named at
      // initialisation — a subject created in this process by the work the
      // event started — without a second process to park in.
      let self = case selector {
        None -> self
        Some(selector) ->
          Self(..self, selector: process.map_selector(selector, Received))
      }

      // Structural equality is the entire definition of a state change, and
      // it is gen_statem's: moving to the state the machine is already in
      // cancels nothing, replays nothing and enters nothing.
      case self.state == from {
        True -> {
          let self = Self(..self, queue: list.append(injected, self.queue))
          loop(apply_timeouts(self, timeouts))
        }
        False -> changed_state(self, from, injected, timeouts)
      }
    }
  }
}

/// Re-queue the event in hand, if the step asked for it.
///
/// The postponed queue is kept newest-first so that this is a cons; arrival
/// order is restored once, by the single `list.reverse` at replay. An event
/// postponed by a step that is itself a state change lands at the back of
/// the queue and is redelivered immediately, in the new state, which is the
/// order a reader would expect from "it arrived last".
fn hold(
  self: Self(state, data, message),
  postponed: Bool,
  current: List(message),
) -> Self(state, data, message) {
  case postponed {
    False -> self
    True -> Self(..self, postponed: list.append(current, self.postponed))
  }
}

/// Apply a step's target: new data, and a new state if it named one.
fn retarget(
  self: Self(state, data, message),
  target: Target(state, data),
) -> Self(state, data, message) {
  case target {
    Keeping(data:) -> Self(..self, data:)
    Moving(state:, data:) -> Self(..self, state:, data:)
  }
}

/// Finish a step that changed the state.
///
/// Three things happen here, and the order between them is the contract:
///
/// 1. The state timeout is cancelled, *before* the step's own timer actions
///    run, so that a state timeout armed by the very transition that leaves
///    the old state survives into the new one.
/// 2. Postponed events are moved to the front of the queue, behind the
///    messages this step injected. Arrival order is restored by the single
///    reverse; the postponed queue is emptied, which is what makes replay
///    exactly-once.
/// 3. The enter callback runs, and anything it injects goes in front of all
///    of it — the depth-first rule, applied to the block that was written
///    last.
fn changed_state(
  self: Self(state, data, message),
  from: state,
  injected: List(message),
  timeouts: List(TimeoutAction(message)),
) -> ExitReason {
  let self = disarm(self, StateTimeout)
  let replayed = list.reverse(self.postponed)
  let queue = list.append(injected, list.append(replayed, self.queue))
  let self = apply_timeouts(Self(..self, queue:, postponed: []), timeouts)
  enter(self, from:, to: self.state)
}

/// Run the enter callback, if there is one, and interpret what it returns.
///
/// An enter callback that transitions comes back through `changed_state` and
/// so runs another enter call. Nothing bounds that chain but the callback
/// itself; a callback that always transitions is a machine that never reads
/// its mailbox, which is the same live-lock `then_handle` can build and is
/// equally the programmer's to avoid.
fn enter(
  self: Self(state, data, message),
  from from: state,
  to to: state,
) -> ExitReason {
  case self.on_enter {
    None -> loop(self)
    Some(handler) -> commit(self, handler(from, to, self.data), [])
  }
}

/// Apply a step's timer changes, in the order they were written.
fn apply_timeouts(
  self: Self(state, data, message),
  actions: List(TimeoutAction(message)),
) -> Self(state, data, message) {
  use self, action <- list.fold(actions, self)
  case action {
    Arm(key:, after_ms:, message:) -> arm(self, key, after_ms, message)
    Disarm(key:) -> disarm(self, key)
  }
}

/// Arm one timer, replacing whatever was armed under the same key.
///
/// The arming record is updated in step with the book, because the book is
/// opaque and a resume has to be able to reconstruct what was running.
fn arm(
  self: Self(state, data, message),
  key: TimerKey,
  after_ms: Int,
  message: message,
) -> Self(state, data, message) {
  Self(
    ..self,
    timers: timer.set(self.timers, for: key, after: after_ms, sending: message),
    arming: dict.insert(self.arming, key, Arming(after_ms:, message:)),
  )
}

/// Disarm one timer, whether or not anything was armed under the key.
///
/// A fire that beat this into the mailbox is not recalled — it cannot be —
/// but the book will now classify it as stale, and `handle_fired` drops it.
fn disarm(
  self: Self(state, data, message),
  key: TimerKey,
) -> Self(state, data, message) {
  Self(
    ..self,
    timers: timer.cancel(self.timers, key),
    arming: dict.delete(self.arming, key),
  )
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
