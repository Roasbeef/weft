//// Named-timer bookkeeping for weft's receive loops.
////
//// Every loop in weft that answers a timeout — the actor's idle timeout
//// today, the state machine's state, event and named timeouts tomorrow —
//// has the same race to lose. `erlang:send_after` puts a message in the
//// owner's mailbox at a wall-clock moment nobody controls, so a timer can
//// fire in the window between the loop deciding to cancel it and the cancel
//// actually running. `erlang:cancel_timer` reports that window honestly
//// (`TimerNotFound` when the message is already in the mailbox) but it
//// cannot take the message back, and the loop cannot selectively receive it
//// away without also swallowing whatever else shares that subject. Left
//// alone, the stale fire arrives one message later and the consumer handles
//// a timeout that was cancelled: a state machine re-enters a state it left,
//// an idle actor is told it is idle in the middle of a burst.
////
//// This module closes that window by making a stale fire *recognisable*
//// rather than trying to make it impossible. Every `set` stamps the
//// outgoing message with a generation number that only that arming ever
//// carries, and the live generation for each key is kept here. A `Fired`
//// message is only ever handed to the consumer by `accept`, which compares
//// the stamp against the book: a fire whose key was cancelled, or whose key
//// was re-armed since, does not match and is dropped. "Cancel with flush"
//// is therefore two halves that must both be present — `cancel` stops the
//// timer it can still stop, and `accept` is where the one it could not stop
//// dies. A loop that selects `Fired` messages straight into its handler,
//// bypassing `accept`, has the bug back.
////
//// The bookkeeping itself is a dict and an integer, and every effect is a
//// single explicit call at the point the dict changes, so the ordering that
//// matters is readable in one screen: cancel the old timer *before*
//// recording the new generation, and delete the entry *before* handing the
//// message on, so a duplicate fire for the same generation cannot be
//// delivered twice.
////
//// Which clock the arming rides is the book's one configurable decision
//// (`weft/timer`'s `Source`, taken at `new_on`), and it is deliberately
//// confined to `set`: a book on an injected source stamps, flushes and
//// accounts for its timers exactly as a wall-clock one does. The one thing
//// it cannot do is stop an arming — an injected `after` returns no handle —
//// which is why stale-by-generation is load-bearing rather than a
//// belt-and-braces second line. A book that recognised a stale fire only
//// *sometimes* would be unsound under injection; this one recognises every
//// one of them, so the missing cancel costs a wasted message and nothing
//// more.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import weft/timer.{type Source, Injected, WallClock}

/// A timer message as it lands in the owner's mailbox.
///
/// The consumer selects for this record and passes it straight to `accept`;
/// the `generation` stamp is what tells a live fire from a cancelled one and
/// is not meaningful to the consumer otherwise.
pub type Fired(key, message) {
  Fired(
    /// The key the timer was armed under.
    key: key,
    /// The arming that produced this message. Only the most recent arming
    /// of a key is live; every earlier stamp is stale by construction.
    generation: Int,
    /// The payload the consumer asked to be sent back to itself.
    message: message,
  )
}

/// One live timer: whatever the source gave back to cancel it with, and the
/// stamp its message carries.
type Entry {
  Entry(handle: Handle, generation: Int)
}

/// What `cancel` can still do about one arming.
///
/// A two-variant type rather than an `Option(process.Timer)` because the
/// question a reader of `stop` is asking is not "is there a timer" but "can
/// this arming be recalled at all", and the answer is a property of the
/// source that armed it.
type Handle {
  /// A wall-clock arming, stoppable by its own BEAM timer handle until the
  /// moment it fires.
  Cancellable(timer: process.Timer)

  /// An injected arming. The wake is in the source's hands and no longer in
  /// the book's, so nothing can stop it and `accept` is where it dies.
  Uncancellable
}

/// The set of timers a single receive loop owns.
///
/// It is a plain value threaded through the loop's state, so a loop that
/// forgets to keep the returned book loses the ability to recognise stale
/// fires — the type is opaque so that the generation counter can never be
/// rewound by a caller, and so that the source cannot be swapped under a
/// set of timers already armed on the other one.
pub opaque type Timers(key, message) {
  Timers(
    subject: Subject(Fired(key, message)),
    source: Source,
    entries: Dict(key, Entry),
    next_generation: Int,
  )
}

/// What `accept` decided about a `Fired` message.
pub type Delivery(key, message) {
  /// The fire is live: the timer was still armed under this generation.
  /// The entry has already been removed from the returned book, since a
  /// one-shot timer fires exactly once.
  Deliver(timers: Timers(key, message), key: key, message: message)

  /// The fire is stale: its key was cancelled, or re-armed after this
  /// message was already on its way. This is the flush — the message dies
  /// here rather than reaching the consumer's handler.
  Stale(timers: Timers(key, message))
}

/// Create an empty timer book on the wall clock, delivering to `subject`.
///
/// The subject is an argument rather than something created here because
/// its owner must be the process that runs the receive loop: a book created
/// in the parent and used in the child would arm timers that fire into the
/// wrong mailbox. Making the caller name the subject keeps that ownership
/// decision visible at the call site.
///
/// ## Examples
///
/// ```gleam
/// let timers = timer.new(process.new_subject())
/// // -> an empty book on the wall clock, nothing armed
/// ```
pub fn new(subject: Subject(Fired(key, message))) -> Timers(key, message) {
  new_on(subject, WallClock)
}

/// Create an empty timer book arming through `source`.
///
/// The source is fixed at construction rather than passed to each `set`,
/// because it is a property of the loop and not of one timeout: a loop
/// arming half its timers on a simulated clock and half on the wall clock
/// would have no coherent notion of when anything happens, and no consumer
/// has asked for one. Fixing it here also means every later call in this
/// module can be read without asking which clock it is on.
///
/// ## Examples
///
/// ```gleam
/// let timers = timer.new_on(process.new_subject(), timer.WallClock)
/// // -> exactly what `timer.new(process.new_subject())` builds
/// ```
///
/// ```gleam
/// let timers =
///   timer.new_on(process.new_subject(), timer.Injected(after: wheel_after))
/// // -> an empty book whose timers fire when the wheel says so
/// ```
pub fn new_on(
  subject: Subject(Fired(key, message)),
  source: Source,
) -> Timers(key, message) {
  Timers(subject:, source:, entries: dict.new(), next_generation: 0)
}

/// The subject timers fire into.
///
/// The loop needs this to add a selector arm for `Fired` messages.
///
/// ## Examples
///
/// ```gleam
/// let selector =
///   process.new_selector()
///   |> process.select_map(timer.subject(timers), Tick)
/// ```
pub fn subject(timers: Timers(key, message)) -> Subject(Fired(key, message)) {
  timers.subject
}

/// Arm a timer under `key`, replacing any timer already armed under it.
///
/// Re-arming is the common case — a loop timeout is reset by every message —
/// so `set` cancels first and stamps a fresh generation second. The order
/// matters: the new generation is what makes the cancelled timer's message
/// recognisable if it was already in flight, so the stamp must be taken
/// after the old entry is gone and never reused. On an injected source the
/// cancel stops nothing at all, so that stamp is the only thing keeping the
/// replaced arming's wake out of the consumer's handler.
///
/// ## Examples
///
/// ```gleam
/// let timers = timer.set(timers, for: IdleTimer, after: 5000, sending: Idle)
/// // -> `Fired(IdleTimer, _, Idle)` arrives in five seconds unless cancelled
/// ```
pub fn set(
  timers: Timers(key, message),
  for key: key,
  after ms: Int,
  sending message: message,
) -> Timers(key, message) {
  let timers = cancel(timers, key)
  let generation = timers.next_generation

  // The one call in the book that knows which clock it is on. Everything
  // after it — the stamp, the entry, the flush in `accept` — is written once
  // for both sources, which is what makes an injected source a
  // configuration choice rather than a second implementation.
  let handle = arm(timers, ms, Fired(key:, generation:, message:))

  let entries = dict.insert(timers.entries, key, Entry(handle:, generation:))
  Timers(..timers, entries:, next_generation: generation + 1)
}

/// Ask the book's source to deliver `fired` after `ms`, and report what can
/// be done about it afterwards.
///
/// The injected branch performs the send inside the wake rather than here:
/// the wake runs in whatever process the source chooses at whatever moment
/// it chooses, and the arming call itself must not wait for either.
fn arm(
  timers: Timers(key, message),
  ms: Int,
  fired: Fired(key, message),
) -> Handle {
  case timers.source {
    WallClock -> Cancellable(process.send_after(timers.subject, ms, fired))

    Injected(after:) -> {
      after(ms, fn() { process.send(timers.subject, fired) })
      Uncancellable
    }
  }
}

/// Cancel the timer armed under `key`, if there is one.
///
/// On the wall clock this stops a timer that has not fired yet. A timer that
/// fired in the window before this call has already put its message in the
/// mailbox and cannot be recalled; dropping the entry here is what makes
/// `accept` classify that message as `Stale`. On an injected source *every*
/// cancel is that window — there is no handle to stop the wake with — so
/// dropping the entry is the entire operation. Cancelling an unarmed key is
/// a no-op either way, which is what lets a loop cancel unconditionally on
/// a state change.
///
/// ## Examples
///
/// ```gleam
/// let timers = timer.cancel(timers, for: IdleTimer)
/// // -> nothing armed under IdleTimer; a fire already in flight is stale
/// ```
pub fn cancel(
  timers: Timers(key, message),
  for key: key,
) -> Timers(key, message) {
  case dict.get(timers.entries, key) {
    Error(Nil) -> timers

    Ok(entry) -> {
      stop(entry.handle)
      Timers(..timers, entries: dict.delete(timers.entries, key))
    }
  }
}

/// Stop whatever can be stopped about one arming.
///
/// Neither branch can report anything the caller could act on, which is the
/// point: the entry has to go either way, and its absence is precisely the
/// signal `accept` uses to discard a fire the cancel did not beat.
fn stop(handle: Handle) -> Nil {
  case handle {
    // The result is deliberately unexamined. `Cancelled` means the message
    // will never arrive and `TimerNotFound` means it may already be in the
    // mailbox, but the book cannot act differently on the two.
    Cancellable(timer:) -> {
      let _ = process.cancel_timer(timer)
      Nil
    }

    // Nothing to stop: an injected arming was handed out as a closure and
    // no handle came back, so this wake will arrive and `accept` will drop
    // it. Under this source that is the whole of cancellation.
    Uncancellable -> Nil
  }
}

/// Cancel every armed timer.
///
/// Used when a loop leaves the state that armed them, or shuts down — and
/// by a suspension, which disarms everything and re-arms from the loop's
/// own arming records on resume. Each key goes through `cancel`, so the
/// wall-clock and injected halves of that are the same code.
///
/// ## Examples
///
/// ```gleam
/// let timers = timer.cancel_all(timers)
/// // -> nothing armed; every fire still in flight is stale
/// ```
pub fn cancel_all(timers: Timers(key, message)) -> Timers(key, message) {
  use book, key, _entry <- dict.fold(timers.entries, timers)
  cancel(book, key)
}

/// Whether a timer is currently armed under `key`.
///
/// ## Examples
///
/// ```gleam
/// let timers = timer.set(timers, for: Retry, after: 100, sending: Retry)
/// assert timer.is_set(timers, Retry)
/// ```
pub fn is_set(timers: Timers(key, message), key: key) -> Bool {
  dict.has_key(timers.entries, key)
}

/// Classify a `Fired` message and, when it is live, hand back its payload.
///
/// This is the second half of cancel-with-flush and the only supported way
/// to consume a timer message. A fire is live only when its key is still
/// armed *and* carries the generation this message was stamped with, so a
/// cancelled timer and a re-armed timer are both recognised as stale and
/// dropped. The entry is deleted before the payload is handed on, because
/// these are one-shot timers and a second fire under the same generation —
/// however it arose — must not reach the consumer twice.
///
/// Under an injected source this is not the second line of defence but the
/// only one: nothing stopped the superseded arming, so every wake the
/// source ever makes arrives here and is classified.
///
/// ## Examples
///
/// ```gleam
/// case timer.accept(timers, fired) {
///   timer.Deliver(timers:, key: _, message:) -> handle(message, timers)
///   timer.Stale(timers:) -> loop(timers)
/// }
/// ```
pub fn accept(
  timers: Timers(key, message),
  fired: Fired(key, message),
) -> Delivery(key, message) {
  case dict.get(timers.entries, fired.key) {
    // The key is not armed at all: it was cancelled, or this is a second
    // fire for a generation already delivered.
    Error(Nil) -> Stale(timers)

    Ok(Entry(generation:, handle: _)) if generation == fired.generation ->
      Deliver(
        timers: Timers(
          ..timers,
          entries: dict.delete(timers.entries, fired.key),
        ),
        key: fired.key,
        message: fired.message,
      )

    // The key is armed, but under a later generation: this message belongs
    // to an arming that was replaced while it was in flight.
    Ok(Entry(generation: _, handle: _)) -> Stale(timers)
  }
}
