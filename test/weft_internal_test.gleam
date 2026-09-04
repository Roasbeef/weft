//// Tests for the internal machinery weft's loops share.
////
//// The timer book's whole reason to exist is a race, so most of these
//// tests deliberately let a timer fire and *then* cancel or re-arm it,
//// which is the ordering a real loop only hits occasionally and always at
//// the worst moment.
////
//// The injected-source tests at the end of the file need no race at all,
//// which is the point of them: under `weft/timer`'s `Injected` a cancel
//// stops nothing, so the wake of a cancelled or replaced arming is always
//// still to come and the test simply runs it. They are also the only tests
//// here that prove a fire happened *because* somebody asked for it, since a
//// book on a fake wheel never fires on its own.

import gleam/erlang/process.{type Subject}
import gleeunit
import weft/internal/timer
import weft/timer as source

pub fn main() -> Nil {
  gleeunit.main()
}

/// Two keys, so that the book has to keep them apart.
type Key {
  Alarm
  Reminder
}

pub fn a_live_timer_is_delivered_test() -> Nil {
  let subject = process.new_subject()
  let timers =
    timer.new(subject) |> timer.set(for: Alarm, after: 1, sending: "ring")

  assert timer.is_set(timers, Alarm)

  let assert Ok(fired) = process.receive(subject, 500)
    as "an armed timer must fire"
  let assert timer.Deliver(timers:, key:, message:) =
    timer.accept(timers, fired)
    as "a timer that was never cancelled must be live"

  assert key == Alarm
  assert message == "ring"

  // One-shot: the entry is gone once it has been delivered, so a duplicate
  // fire cannot be handled twice.
  assert !timer.is_set(timers, Alarm)
}

pub fn a_cancelled_timer_that_already_fired_is_flushed_test() -> Nil {
  let subject = process.new_subject()
  let timers =
    timer.new(subject) |> timer.set(for: Alarm, after: 0, sending: "ring")

  // Let the message actually land in the mailbox before cancelling. This is
  // the window `erlang:cancel_timer` cannot close and the generation stamp
  // exists for.
  process.sleep(20)
  let timers = timer.cancel(timers, for: Alarm)

  let assert Ok(fired) = process.receive(subject, 500)
    as "the message was already in the mailbox when the timer was cancelled"
  let assert timer.Stale(timers:) = timer.accept(timers, fired)
    as "a fire that beat its own cancel must not reach the consumer"

  assert !timer.is_set(timers, Alarm)
}

pub fn re_arming_makes_the_previous_fire_stale_test() -> Nil {
  let subject = process.new_subject()
  let timers =
    timer.new(subject) |> timer.set(for: Alarm, after: 0, sending: "first")

  // The first arming fires, and only then is the key re-armed: the loop
  // timeout reset that a message arriving at the wrong instant produces.
  process.sleep(20)
  let timers = timer.set(timers, for: Alarm, after: 0, sending: "second")

  let assert Ok(first) = process.receive(subject, 500)
    as "the first arming fired before it was replaced"
  let assert timer.Stale(timers:) = timer.accept(timers, first)
    as "a replaced arming is stale even though its key is armed"

  let assert Ok(second) = process.receive(subject, 500)
    as "the second arming must fire too"
  let assert timer.Deliver(timers: _, key: _, message:) =
    timer.accept(timers, second)
    as "the current arming is live"

  assert message == "second"
}

pub fn cancelling_one_key_leaves_the_others_armed_test() -> Nil {
  let subject = process.new_subject()
  let timers =
    timer.new(subject)
    |> timer.set(for: Alarm, after: 50, sending: "ring")
    |> timer.set(for: Reminder, after: 50, sending: "nudge")
    |> timer.cancel(for: Alarm)

  assert !timer.is_set(timers, Alarm)
  assert timer.is_set(timers, Reminder)

  let assert Ok(fired) = process.receive(subject, 500)
    as "the reminder was never cancelled"
  let assert timer.Deliver(timers: _, key:, message:) =
    timer.accept(timers, fired)
    as "the reminder is live"

  assert key == Reminder
  assert message == "nudge"
}

pub fn cancelling_an_unarmed_key_is_a_no_op_test() -> Nil {
  let subject = process.new_subject()
  let timers = timer.new(subject) |> timer.cancel(for: Alarm)

  assert !timer.is_set(timers, Alarm)
  assert process.receive(subject, 20) == Error(Nil)
}

pub fn cancel_all_disarms_everything_test() -> Nil {
  let subject = process.new_subject()
  let timers =
    timer.new(subject)
    |> timer.set(for: Alarm, after: 0, sending: "ring")
    |> timer.set(for: Reminder, after: 0, sending: "nudge")

  // Both fires are already in the mailbox by the time they are cancelled,
  // so both must be recognised as stale rather than delivered.
  process.sleep(20)
  let timers = timer.cancel_all(timers)

  let assert Ok(first) = process.receive(subject, 500) as "the alarm fired"
  let assert timer.Stale(timers:) = timer.accept(timers, first)
    as "a cancelled alarm is stale"
  let assert Ok(second) = process.receive(subject, 500) as "the reminder fired"
  let assert timer.Stale(timers:) = timer.accept(timers, second)
    as "a cancelled reminder is stale"

  assert !timer.is_set(timers, Alarm)
  assert !timer.is_set(timers, Reminder)
}

pub fn the_book_reports_the_subject_it_fires_into_test() -> Nil {
  let subject = process.new_subject()
  let timers: timer.Timers(Key, String) = timer.new(subject)

  assert timer.subject(timers) == subject
}

// ------------------------------------------------- the injected timer source

/// One arming the book asked an injected source for.
///
/// The wake is kept rather than run, because the whole point of an injected
/// source is that somebody else decides when a timer fires. Every test
/// below is that decision, made by hand at the moment that makes the
/// stale-fire question interesting.
type Arming {
  Arming(delay_ms: Int, wake: fn() -> Nil)
}

/// A source that records each arming into `armings` and fires nothing.
///
/// This is the fake wheel in its smallest honest form: a book on it never
/// fires on its own, however long the test waits, so anything delivered is
/// something the test ran.
fn recording_source(into armings: Subject(Arming)) -> source.Source {
  source.Injected(after: fn(delay_ms, wake) {
    process.send(armings, Arming(delay_ms:, wake:))
  })
}

pub fn an_injected_fire_is_delivered_test() -> Nil {
  let subject = process.new_subject()
  let armings = process.new_subject()
  let timers =
    timer.new_on(subject, recording_source(armings))
    |> timer.set(for: Alarm, after: 40, sending: "ring")

  let assert Ok(arming) = process.receive(armings, 500)
    as "the book must ask the source to arm, synchronously in `set`"
  assert arming.delay_ms == 40

  // Nothing is in the mailbox yet, and no amount of waiting would put it
  // there: the wall clock does not arm this book's timers.
  assert process.receive(subject, 20) == Error(Nil)

  arming.wake()

  let assert Ok(fired) = process.receive(subject, 500)
    as "the wake sends the fire the book prepared"
  let assert timer.Deliver(timers:, key:, message:) =
    timer.accept(timers, fired)
    as "a fire from the live arming must be delivered"

  assert key == Alarm
  assert message == "ring"
  assert !timer.is_set(timers, Alarm)
}

pub fn re_arming_an_injected_timer_makes_the_first_wake_stale_test() -> Nil {
  let subject = process.new_subject()
  let armings = process.new_subject()
  let timers =
    timer.new_on(subject, recording_source(armings))
    |> timer.set(for: Alarm, after: 40, sending: "first")
    |> timer.set(for: Alarm, after: 40, sending: "second")

  let assert Ok(first) = process.receive(armings, 500)
    as "the first arming was asked for"
  let assert Ok(second) = process.receive(armings, 500)
    as "the second arming was asked for"

  // The replaced arming's wake is still out there — an injected source
  // returns no handle and the book cancelled nothing — so it is run here on
  // purpose. It has to die in the generation check.
  first.wake()

  let assert Ok(stale) = process.receive(subject, 500)
    as "a superseded injected arming still fires"
  let assert timer.Stale(timers:) = timer.accept(timers, stale)
    as "a superseded arming is stale even though its key is armed"

  second.wake()

  let assert Ok(live) = process.receive(subject, 500)
    as "the current arming fires too"
  let assert timer.Deliver(timers: _, key: _, message:) =
    timer.accept(timers, live)
    as "the current arming is live"

  assert message == "second"
}

pub fn a_cancelled_injected_timer_is_flushed_test() -> Nil {
  let subject = process.new_subject()
  let armings = process.new_subject()
  let timers =
    timer.new_on(subject, recording_source(armings))
    |> timer.set(for: Alarm, after: 40, sending: "ring")
    |> timer.cancel(for: Alarm)

  let assert Ok(arming) = process.receive(armings, 500)
    as "the arming happened before the cancel"

  // Under injection this is not a race the test had to win: the cancel can
  // never stop the wake, so running it after the cancel is the ordinary
  // case rather than the unlucky one.
  arming.wake()

  let assert Ok(fired) = process.receive(subject, 500)
    as "a cancelled injected arming still fires"
  let assert timer.Stale(timers:) = timer.accept(timers, fired)
    as "a cancelled arming must not reach the consumer"

  assert !timer.is_set(timers, Alarm)
}

pub fn cancel_all_flushes_every_injected_wake_test() -> Nil {
  let subject = process.new_subject()
  let armings = process.new_subject()
  let timers =
    timer.new_on(subject, recording_source(armings))
    |> timer.set(for: Alarm, after: 40, sending: "ring")
    |> timer.set(for: Reminder, after: 40, sending: "nudge")
    |> timer.cancel_all

  let assert Ok(first) = process.receive(armings, 500) as "the alarm was armed"
  let assert Ok(second) = process.receive(armings, 500)
    as "the reminder was armed"

  first.wake()
  second.wake()

  // This is the shape a suspension leaves behind: everything disarmed, and
  // two wakes in flight that nothing can recall.
  let assert Ok(alarm) = process.receive(subject, 500) as "the alarm fired"
  let assert timer.Stale(timers:) = timer.accept(timers, alarm)
    as "a disarmed alarm is stale"
  let assert Ok(reminder) = process.receive(subject, 500)
    as "the reminder fired"
  let assert timer.Stale(timers:) = timer.accept(timers, reminder)
    as "a disarmed reminder is stale"

  assert !timer.is_set(timers, Alarm)
  assert !timer.is_set(timers, Reminder)
}
