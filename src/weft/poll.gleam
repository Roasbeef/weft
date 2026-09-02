//// Bounded polling in the caller's own process.
////
//// Some waits cannot be handed to another process: a launcher that needs
//// a lock before it can do anything else, a caller reaching into a tree
//// that may be mid-restart, a shell that must know whether a server came
//// up before it prints the address. The waiting process *is* the one that
//// needs the answer, the thing it is waiting on is a synchronous probe
//// rather than a message, and the only honest bound is the wall clock.
//// Every such site ends up hand-rolling the same loop — try, check the
//// deadline, sleep, recurse — and every copy re-decides the same three
//// corners: whether the first attempt sleeps first, whether an attempt is
//// made at the deadline itself, and how a probe that failed for good is
//// told apart from one that merely has not succeeded yet.
////
//// `until` is that loop, decided once. The first attempt is immediate. An
//// attempt that says `Retry` is followed by a sleep of `every`
//// milliseconds, clipped to what remains of the budget, and then another
//// attempt; when the budget is spent a last attempt is still made, so a
//// probe that would have succeeded exactly at the deadline is not reported
//// as expired. `Fail` ends the wait at once with the caller's own error,
//// and `Expired` is a distinct outcome rather than an error the caller has
//// to invent — "did not happen in time" is a fact about the clock, not
//// about the probe.
////
//// ## Whose clock, and what the wait carries
////
//// `until` measures the monotonic clock and sleeps with `process.sleep`,
//// which is what a wait on a real resource wants. Some callers do not have
//// that option: a system with an injected time capability runs its waits on
//// a logical clock its own simulation steps, and a wait that consulted the
//// operating system instead would either hang a simulated run or make it
//// non-deterministic. `Clock` is that pair of decisions — how to read the
//// time and how to rest — made a value, `monotonic()` is the one `until`
//// uses, and `until_on` is `until` with the caller's own.
////
//// The other thing a hand-rolled loop does that a `fn() -> Attempt` cannot
//// is carry something between attempts: the handles already settled, the
//// epoch the last exchange reported, the refusal to hand back if the budget
//// runs out. `fold_until` is the same loop with the probe threading a state
//// value, and `until_on` is the special case where that value is `Nil`. A
//// state carried this way is also what makes expiry informative: `RanOut`
//// hands back the state as the last attempt left it, so a caller who has
//// been accumulating an answer keeps it rather than starting again.
////
//// A long wait should not probe as hard as a short one, so the interval is
//// an `Interval` rather than a number: `Fixed` for a wait bounded in
//// milliseconds, `Doubling` for one bounded in minutes, where a fixed
//// interval is the difference between a few probes and a few thousand.
////
//// This module deliberately owns no process: it sleeps in the caller,
//// which is the whole point. A wait that could be a message should be a
//// `weft/state_machine` state with a timeout instead.
////
//// ## Example
////
//// ```gleam
//// import weft/poll
////
//// pub fn await_lock(path: String) -> Result(Lock, String) {
////   case
////     poll.until(within: 5000, every: 25, attempt: fn() {
////       case try_lock(path) {
////         Ok(lock) -> poll.Done(lock)
////         Error("busy") -> poll.Retry
////         Error(reason) -> poll.Fail(reason)
////       }
////     })
////   {
////     poll.Answered(lock) -> Ok(lock)
////     poll.Failed(reason) -> Error("acquire lock: " <> reason)
////     poll.Expired -> Error("timed out waiting for the lock")
////   }
//// }
//// ```

import gleam/erlang/process
import gleam/int

/// How a bounded wait reads the time and how it rests.
///
/// Both halves are injected together because they are one decision: a wait
/// that measured a simulated clock and slept on the real one would burn
/// wall time to make no logical progress, and a wait that measured the real
/// clock and rested by stepping a simulated one would never end. `now`
/// returns a millisecond timestamp on whatever base the caller uses — the
/// only thing this module does with it is subtract two readings — and
/// `sleep` must pass roughly that many of the same milliseconds before it
/// returns.
///
/// ## Examples
///
/// ```gleam
/// // A wait on a session's own injected time capability, resting by
/// // stepping the simulation rather than by sleeping.
/// poll.Clock(now: fn() { session_now() }, sleep: fn(ms) { advance(ms) })
/// ```
pub type Clock {
  Clock(
    /// The current time in milliseconds, on the caller's own base.
    now: fn() -> Int,
    /// Rest for this many milliseconds of that same base.
    sleep: fn(Int) -> Nil,
  )
}

/// The clock `until` uses: `erlang:monotonic_time` and `process.sleep`.
///
/// Monotonic rather than system time, so a wall-clock step cannot lengthen
/// or shorten a wait.
///
/// ## Examples
///
/// ```gleam
/// poll.until_on(clock: poll.monotonic(), within: 5000, every: poll.Fixed(25), attempt:)
/// // -> exactly what `poll.until(within: 5000, every: 25, attempt:)` does
/// ```
pub fn monotonic() -> Clock {
  Clock(now: monotonic_ms, sleep: process.sleep)
}

/// How long a wait rests between attempts.
///
/// The choice is about what a wait costs whatever it is probing. A wait
/// bounded in milliseconds wants the same short gap every time; a wait
/// bounded in minutes wants to stop asking so often once it is clear the
/// answer is not imminent, and doubling is the cheapest schedule that does
/// that without a parameter nobody can pick.
pub type Interval {
  /// The same gap after every attempt.
  Fixed(
    /// Milliseconds between attempts; anything below one is treated as one.
    ms: Int,
  )

  /// Doubling from `from`, capped at `to`. A wait that ends quickly pays
  /// `from`; one that runs long settles at `to`.
  Doubling(
    /// The first gap, in milliseconds.
    from: Int,
    /// The longest gap the doubling reaches, in milliseconds.
    to: Int,
  )
}

/// What one attempt of the probe reports.
pub type Attempt(a, e) {
  /// The probe succeeded; the wait is over.
  Done(
    /// The value the probe produced.
    value: a,
  )

  /// Not yet. Sleep and try again, budget permitting.
  Retry

  /// The probe failed in a way that more time will not fix. The wait ends
  /// at once, carrying the caller's own error.
  Fail(
    /// Why the probe cannot succeed.
    error: e,
  )
}

/// How a bounded wait ended.
pub type Outcome(a, e) {
  /// An attempt reported `Done`.
  Answered(
    /// The value that attempt produced.
    value: a,
  )

  /// An attempt reported `Fail`.
  Failed(
    /// The error that attempt carried.
    error: e,
  )

  /// Every attempt inside the budget, including the last one made at the
  /// deadline, reported `Retry`.
  Expired
}

/// What one attempt of a probe that carries work forward reports.
///
/// The same three answers as `Attempt`, except that "not yet" hands back
/// the state the next attempt should see. A probe that has nothing to carry
/// wants `Attempt` and `until_on`.
pub type Pass(a, e, state) {
  /// The probe succeeded; the wait is over.
  Settled(
    /// The value the probe produced.
    value: a,
  )

  /// Not yet. Sleep and try again, budget permitting, with this state.
  Pending(
    /// What the next attempt is given.
    state: state,
  )

  /// The probe failed in a way that more time will not fix.
  Broken(
    /// Why the probe cannot succeed.
    error: e,
  )
}

/// How a bounded wait that carried state ended.
///
/// `RanOut` differs from `Expired` in exactly one way, and it is the reason
/// `fold_until` exists: it hands back the state as the last attempt left it.
/// A caller who has been accumulating a partial answer — the handles that
/// did settle, the refusal the last exchange reported — keeps it rather
/// than being told only that time ran out.
pub type Verdict(a, e, state) {
  /// An attempt reported `Settled`.
  Answer(
    /// The value that attempt produced.
    value: a,
  )

  /// An attempt reported `Broken`.
  Failure(
    /// The error that attempt carried.
    error: e,
  )

  /// Every attempt inside the budget, including the last one made at the
  /// deadline, reported `Pending`.
  RanOut(
    /// The state the last attempt left behind.
    state: state,
  )
}

/// Poll `attempt` until it answers, fails, or `within` milliseconds have
/// passed, sleeping `every` milliseconds between attempts.
///
/// The first attempt is immediate, a final attempt is made when the budget
/// runs out, and the sleep never overshoots the deadline. A `within` of
/// zero therefore still makes exactly one attempt. Time is measured on the
/// monotonic clock, so a wall-clock step cannot lengthen or shorten the
/// wait; a caller whose time base is its own uses `until_on`, and one whose
/// probe has something to tell its successor uses `fold_until`.
///
/// ## Examples
///
/// ```gleam
/// poll.until(within: 1000, every: 50, attempt: fn() {
///   case probe() {
///     Ok(value) -> poll.Done(value)
///     Error(Nil) -> poll.Retry
///   }
/// })
/// // -> poll.Answered(value), or poll.Expired after about a second
/// ```
pub fn until(
  within within: Int,
  every every: Int,
  attempt attempt: fn() -> Attempt(a, e),
) -> Outcome(a, e) {
  until_on(clock: monotonic(), within:, every: Fixed(every), attempt:)
}

/// Poll `attempt` on `clock` rather than on the monotonic clock.
///
/// Every rule `until` fixes still holds — immediate first attempt, a last
/// attempt at the deadline, a sleep clipped to what remains, `Fail` distinct
/// from `Retry` — and all of them are now measured and rested on the
/// caller's own time base. That is what lets a wait belonging to a system
/// with an injected clock run under that system's simulation instead of
/// against the operating system.
///
/// One consequence is worth stating plainly: the wait ends when `clock`
/// says the budget is spent, so a clock whose `now` never moves is a wait
/// that never expires. That is a fact about the clock rather than about
/// this function, and a caller handing in a fixture clock should hand in
/// one that advances.
///
/// ## Examples
///
/// ```gleam
/// poll.until_on(
///   clock: session_clock,
///   within: 30_000,
///   every: poll.Doubling(from: 25, to: 250),
///   attempt: fn() {
///     case read_record() {
///       Ok(record) -> poll.Done(record)
///       Error(Nil) -> poll.Retry
///     }
///   },
/// )
/// ```
pub fn until_on(
  clock clock: Clock,
  within within: Int,
  every every: Interval,
  attempt attempt: fn() -> Attempt(a, e),
) -> Outcome(a, e) {
  // The state is `Nil` and the probe ignores it, which is the whole
  // difference between the two entry points: one loop, and a caller who has
  // nothing to carry does not have to say so at every attempt.
  let folded =
    fold_until(clock:, within:, every:, from: Nil, attempt: fn(_state) {
      case attempt() {
        Done(value:) -> Settled(value:)
        Fail(error:) -> Broken(error:)
        Retry -> Pending(Nil)
      }
    })

  case folded {
    Answer(value:) -> Answered(value:)
    Failure(error:) -> Failed(error:)
    RanOut(state: Nil) -> Expired
  }
}

/// Poll `attempt` on `clock`, threading a state value from one attempt to
/// the next.
///
/// This is the loop the other two are written in terms of, and the one to
/// reach for when an attempt has something to tell its successor: the work
/// already done, the token the last exchange handed back, the answer to
/// give up with. Expiry hands that state back as `RanOut` rather than
/// discarding it, so a wait that half-succeeded reports what it got.
///
/// The interval is an `Interval` here rather than a number because a wait
/// that carries state is usually the long one, and a long wait probing at a
/// short fixed interval is the difference between a few dozen reads and a
/// few thousand.
///
/// ## Examples
///
/// ```gleam
/// // Wait for every handle to settle, keeping the ones that already have.
/// poll.fold_until(
///   clock: session_clock,
///   within: 30_000,
///   every: poll.Doubling(from: 25, to: 250),
///   from: dict.new(),
///   attempt: fn(settled) {
///     let settled = list.fold(handles, settled, poll_one)
///     case dict.size(settled) == list.length(handles) {
///       True -> poll.Settled(settled)
///       False -> poll.Pending(settled)
///     }
///   },
/// )
/// // -> poll.Answer(all), or poll.RanOut(the ones that did settle)
/// ```
pub fn fold_until(
  clock clock: Clock,
  within within: Int,
  every every: Interval,
  from state: state,
  attempt attempt: fn(state) -> Pass(a, e, state),
) -> Verdict(a, e, state) {
  let deadline = clock.now() + int.max(0, within)
  loop(clock, deadline, every, first_gap(every), state, attempt)
}

/// One attempt, then the decision the loop makes between attempts: how
/// much budget is left decides whether to sleep the full interval, sleep
/// only what remains and try one last time, or report expiry.
///
/// The clock is read once per pass, after the attempt, so the time the
/// attempt itself took is charged against the budget rather than only the
/// naps. A probe that is slow under exactly the contention this loop exists
/// for would otherwise let a nominal thirty-second wait run for minutes.
fn loop(
  clock: Clock,
  deadline: Int,
  every: Interval,
  gap: Int,
  state: state,
  attempt: fn(state) -> Pass(a, e, state),
) -> Verdict(a, e, state) {
  case attempt(state) {
    Settled(value:) -> Answer(value:)
    Broken(error:) -> Failure(error:)

    Pending(state:) -> {
      let remaining = deadline - clock.now()
      case remaining > 0 {
        False -> RanOut(state:)
        True -> {
          clock.sleep(int.min(gap, remaining))
          loop(clock, deadline, every, next_gap(every, gap), state, attempt)
        }
      }
    }
  }
}

/// The gap before the second attempt.
fn first_gap(every: Interval) -> Int {
  case every {
    Fixed(ms:) -> int.max(1, ms)
    Doubling(from:, to: _) -> int.max(1, from)
  }
}

/// The gap after one that has already been slept.
///
/// A `Doubling` whose `to` is below its `from` is clamped rather than
/// refused: the caller asked for a ceiling and a floor that disagree, and
/// the ceiling is the one that bounds the cost.
fn next_gap(every: Interval, gap: Int) -> Int {
  case every {
    Fixed(ms:) -> int.max(1, ms)
    Doubling(from: _, to:) -> int.min(gap * 2, int.max(1, to))
  }
}

/// `erlang:monotonic_time/1` in milliseconds. The stock BIF's shape lines
/// up with a Gleam signature directly, so no shim module is needed; the
/// single-variant type is the house idiom for naming an atom without
/// building one at runtime.
fn monotonic_ms() -> Int {
  monotonic_time(Millisecond)
}

type TimeUnit {
  Millisecond
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: TimeUnit) -> Int
