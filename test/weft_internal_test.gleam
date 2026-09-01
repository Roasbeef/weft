//// Tests for the internal machinery weft's loops share.
////
//// The timer book's whole reason to exist is a race, so most of these
//// tests deliberately let a timer fire and *then* cancel or re-arm it,
//// which is the ordering a real loop only hits occasionally and always at
//// the worst moment.

import gleam/erlang/process
import gleeunit
import weft/internal/timer

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
