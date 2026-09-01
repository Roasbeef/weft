//// Tests for the bounded poll: the three corners every hand-rolled copy
//// re-decided — immediate first attempt, a last attempt at the deadline,
//// failure told apart from not-yet — plus the budget bound itself.

import gleam/erlang/process
import weft/poll

/// What a test asks its counter.
type Count {
  Bump
  Read(reply: process.Subject(Int))
}

/// A counter the probe closes over, so a test can see how many attempts
/// the loop made. It lives in its own process because the probe runs in
/// the polling process and cannot return an updated value to the test.
fn counter() -> #(fn() -> Nil, fn() -> Int) {
  let handoff = process.new_subject()
  process.spawn_unlinked(fn() {
    let inbox = process.new_subject()
    process.send(handoff, inbox)
    count(inbox, 0)
  })
  let assert Ok(inbox) = process.receive(handoff, 2000)
    as "the counter hands its inbox over at once"
  let bump = fn() { process.send(inbox, Bump) }
  let read = fn() { process.call_forever(inbox, Read) }
  #(bump, read)
}

fn count(inbox: process.Subject(Count), seen: Int) -> Nil {
  case process.receive_forever(inbox) {
    Bump -> count(inbox, seen + 1)
    Read(reply:) -> {
      process.send(reply, seen)
      count(inbox, seen)
    }
  }
}

pub fn an_immediate_answer_makes_exactly_one_attempt_test() -> Nil {
  let #(bump, read) = counter()
  let outcome =
    poll.until(within: 1000, every: 50, attempt: fn() {
      bump()
      poll.Done("now")
    })
  assert outcome == poll.Answered("now")
  assert read() == 1
}

pub fn a_retry_is_followed_by_another_attempt_test() -> Nil {
  let #(bump, read) = counter()
  let outcome =
    poll.until(within: 2000, every: 10, attempt: fn() {
      bump()
      case read() >= 3 {
        True -> poll.Done(Nil)
        False -> poll.Retry
      }
    })
  assert outcome == poll.Answered(Nil)
  assert read() == 3
}

pub fn a_failure_ends_the_wait_at_once_test() -> Nil {
  let #(bump, read) = counter()
  let outcome: poll.Outcome(Nil, String) =
    poll.until(within: 5000, every: 500, attempt: fn() {
      bump()
      poll.Fail("gone")
    })
  assert outcome == poll.Failed("gone")
  assert read() == 1
}

pub fn an_exhausted_budget_still_makes_a_last_attempt_test() -> Nil {
  let #(bump, read) = counter()
  let outcome: poll.Outcome(Nil, Nil) =
    poll.until(within: 0, every: 50, attempt: fn() {
      bump()
      poll.Retry
    })
  assert outcome == poll.Expired
  assert read() == 1
}

pub fn the_sleep_never_overshoots_the_deadline_test() -> Nil {
  let #(bump, read) = counter()
  let outcome: poll.Outcome(Nil, Nil) =
    poll.until(within: 120, every: 1000, attempt: fn() {
      bump()
      poll.Retry
    })
  // One attempt at once, one at the clipped deadline, none a full
  // interval later.
  assert outcome == poll.Expired
  assert read() == 2
}
