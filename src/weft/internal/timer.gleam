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

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}

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

/// One live timer: the handle needed to cancel it, and the stamp its
/// message carries.
type Entry {
  Entry(timer: process.Timer, generation: Int)
}

/// The set of timers a single receive loop owns.
///
/// It is a plain value threaded through the loop's state, so a loop that
/// forgets to keep the returned book loses the ability to recognise stale
/// fires — the type is opaque so that the generation counter can never be
/// rewound by a caller.
pub opaque type Timers(key, message) {
  Timers(
    subject: Subject(Fired(key, message)),
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

/// Create an empty timer book delivering to `subject`.
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
/// // -> an empty book, nothing armed
/// ```
pub fn new(subject: Subject(Fired(key, message))) -> Timers(key, message) {
  Timers(subject:, entries: dict.new(), next_generation: 0)
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
/// after the old entry is gone and never reused.
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
  let timer =
    process.send_after(timers.subject, ms, Fired(key:, generation:, message:))
  let entries = dict.insert(timers.entries, key, Entry(timer:, generation:))
  Timers(..timers, entries:, next_generation: generation + 1)
}

/// Cancel the timer armed under `key`, if there is one.
///
/// This stops a timer that has not fired yet. A timer that fired in the
/// window before this call has already put its message in the mailbox and
/// cannot be recalled; dropping the entry here is what makes `accept`
/// classify that message as `Stale`. Cancelling an unarmed key is a no-op,
/// which is what lets a loop cancel unconditionally on a state change.
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
      // The result is deliberately unexamined. `Cancelled` means the message
      // will never arrive and `TimerNotFound` means it may already be in the
      // mailbox, but the book cannot act differently on the two: the entry
      // has to go either way, and its absence is precisely the signal
      // `accept` uses to discard a fire that beat the cancel.
      let _ = process.cancel_timer(entry.timer)
      Timers(..timers, entries: dict.delete(timers.entries, key))
    }
  }
}

/// Cancel every armed timer.
///
/// Used when a loop leaves the state that armed them, or shuts down.
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

    Ok(Entry(generation:, timer: _)) if generation == fired.generation ->
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
    Ok(Entry(generation: _, timer: _)) -> Stale(timers)
  }
}
