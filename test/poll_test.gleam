//// Tests for the bounded poll: the three corners every hand-rolled copy
//// re-decided — immediate first attempt, a last attempt at the deadline,
//// failure told apart from not-yet — plus the budget bound itself.

import gleam/erlang/process
import gleam/list
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

// -------------------------------------------------------- injected clocks

/// What a test asks its fake clock.
type Tick {
  Now(reply: process.Subject(Int))
  Rest(ms: Int)
  Slept(reply: process.Subject(#(Int, List(Int))))
}

/// A clock that moves only when the wait rests on it.
///
/// This is the whole point of the injected form, made testable: nothing but
/// `sleep` advances it, so a loop that fails to rest cannot reach the
/// deadline by luck and a loop that rests too long is visible in the record
/// of what it slept for. The record comes back oldest first, so a backoff
/// schedule is an assertion rather than an inference from wall time.
fn fake_clock(from: Int) -> #(poll.Clock, fn() -> #(Int, List(Int))) {
  let handoff = process.new_subject()
  process.spawn_unlinked(fn() {
    let inbox = process.new_subject()
    process.send(handoff, inbox)
    tick(inbox, from, [])
  })
  let assert Ok(inbox) = process.receive(handoff, 2000)
    as "the clock hands its inbox over at once"

  let clock =
    poll.Clock(now: fn() { process.call_forever(inbox, Now) }, sleep: fn(ms) {
      process.send(inbox, Rest(ms))
    })
  #(clock, fn() { process.call_forever(inbox, Slept) })
}

fn tick(inbox: process.Subject(Tick), now: Int, slept: List(Int)) -> Nil {
  case process.receive_forever(inbox) {
    Now(reply:) -> {
      process.send(reply, now)
      tick(inbox, now, slept)
    }

    Rest(ms:) -> tick(inbox, now + ms, [ms, ..slept])

    Slept(reply:) -> {
      process.send(reply, #(now, list.reverse(slept)))
      tick(inbox, now, slept)
    }
  }
}

pub fn an_injected_clock_makes_the_first_attempt_immediately_test() -> Nil {
  let #(clock, record) = fake_clock(1000)
  let #(bump, read) = counter()
  let outcome =
    poll.until_on(clock:, within: 5000, every: poll.Fixed(50), attempt: fn() {
      bump()
      poll.Done("now")
    })

  // Nothing was slept, so on this clock no time passed at all: the answer
  // came from an attempt made before the loop rested even once.
  assert outcome == poll.Answered("now")
  assert read() == 1
  assert record() == #(1000, [])
}

pub fn an_injected_clock_still_makes_a_last_attempt_at_the_deadline_test() -> Nil {
  let #(clock, record) = fake_clock(1000)
  let #(bump, read) = counter()
  let outcome: poll.Outcome(Nil, Nil) =
    poll.until_on(clock:, within: 100, every: poll.Fixed(40), attempt: fn() {
      bump()
      poll.Retry
    })

  // 0, 40, 80 and then the clipped 100: four attempts, and the last one is
  // made at the deadline rather than skipped for having arrived on it.
  assert outcome == poll.Expired
  assert read() == 4
  assert record() == #(1100, [40, 40, 20])
}

pub fn an_injected_sleep_never_overshoots_the_deadline_test() -> Nil {
  let #(clock, record) = fake_clock(0)
  let #(bump, read) = counter()
  let outcome: poll.Outcome(Nil, Nil) =
    poll.until_on(clock:, within: 120, every: poll.Fixed(1000), attempt: fn() {
      bump()
      poll.Retry
    })

  // The single rest is the budget, not the interval. A loop that slept the
  // interval would land at 1000 and this would read #(1000, [1000]).
  assert outcome == poll.Expired
  assert read() == 2
  assert record() == #(120, [120])
}

pub fn a_failure_on_an_injected_clock_is_not_a_retry_test() -> Nil {
  let #(clock, record) = fake_clock(0)
  let #(bump, read) = counter()
  let outcome: poll.Outcome(Nil, String) =
    poll.until_on(clock:, within: 5000, every: poll.Fixed(10), attempt: fn() {
      bump()
      poll.Fail("gone")
    })

  // "Cannot succeed" ends the wait where "has not succeeded yet" would have
  // spent the whole budget: one attempt, no rest.
  assert outcome == poll.Failed("gone")
  assert read() == 1
  assert record() == #(0, [])
}

pub fn a_within_of_zero_on_an_injected_clock_makes_one_attempt_test() -> Nil {
  let #(clock, record) = fake_clock(500)
  let #(bump, read) = counter()
  let outcome: poll.Outcome(Nil, Nil) =
    poll.until_on(clock:, within: 0, every: poll.Fixed(50), attempt: fn() {
      bump()
      poll.Retry
    })

  assert outcome == poll.Expired
  assert read() == 1
  assert record() == #(500, [])
}

pub fn a_carried_state_reaches_the_next_attempt_test() -> Nil {
  let #(clock, _record) = fake_clock(0)
  let verdict =
    poll.fold_until(
      clock:,
      within: 5000,
      every: poll.Fixed(10),
      from: [],
      attempt: fn(seen) {
        let seen = [list.length(seen), ..seen]
        case list.length(seen) >= 3 {
          True -> poll.Settled(list.reverse(seen))
          False -> poll.Pending(seen)
        }
      },
    )

  // Each attempt saw exactly what the one before it left, which is the
  // whole difference between this and a probe that closes over nothing.
  assert verdict == poll.Answer([0, 1, 2])
}

pub fn expiry_hands_back_the_state_the_last_attempt_left_test() -> Nil {
  let #(clock, _record) = fake_clock(0)
  let verdict: poll.Verdict(Nil, Nil, Int) =
    poll.fold_until(
      clock:,
      within: 100,
      every: poll.Fixed(40),
      from: 0,
      attempt: fn(attempts) { poll.Pending(attempts + 1) },
    )

  // Four attempts, and the count survives the expiry. `Expired` would have
  // thrown away everything the wait learned.
  assert verdict == poll.RanOut(4)
}

pub fn a_doubling_interval_backs_off_and_stops_at_its_cap_test() -> Nil {
  let #(clock, record) = fake_clock(0)
  let verdict: poll.Verdict(Nil, Nil, Nil) =
    poll.fold_until(
      clock:,
      within: 1000,
      every: poll.Doubling(from: 25, to: 100),
      from: Nil,
      attempt: fn(_state) { poll.Pending(Nil) },
    )

  // 25, 50, 100 and then 100 forever, with the last rest clipped to what
  // the budget had left. A fixed 25 would have rested forty times.
  assert verdict == poll.RanOut(Nil)
  assert record()
    == #(1000, [25, 50, 100, 100, 100, 100, 100, 100, 100, 100, 100, 25])
}

pub fn a_fixed_interval_does_not_back_off_test() -> Nil {
  // The control for the test above: the same budget and the same first gap,
  // with the schedule flat.
  let #(clock, record) = fake_clock(0)
  let verdict: poll.Verdict(Nil, Nil, Nil) =
    poll.fold_until(
      clock:,
      within: 100,
      every: poll.Fixed(25),
      from: Nil,
      attempt: fn(_state) { poll.Pending(Nil) },
    )

  assert verdict == poll.RanOut(Nil)
  assert record() == #(100, [25, 25, 25, 25])
}
