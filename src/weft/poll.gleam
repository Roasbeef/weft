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

/// Poll `attempt` until it answers, fails, or `within` milliseconds have
/// passed, sleeping `every` milliseconds between attempts.
///
/// The first attempt is immediate, a final attempt is made when the budget
/// runs out, and the sleep never overshoots the deadline. A `within` of
/// zero therefore still makes exactly one attempt. Time is measured on the
/// monotonic clock, so a wall-clock step cannot lengthen or shorten the
/// wait.
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
  let deadline = monotonic_ms() + int.max(0, within)
  loop(deadline, int.max(1, every), attempt)
}

/// One attempt, then the decision the loop makes between attempts: how
/// much budget is left decides whether to sleep the full interval, sleep
/// only what remains and try one last time, or report expiry.
fn loop(
  deadline: Int,
  every: Int,
  attempt: fn() -> Attempt(a, e),
) -> Outcome(a, e) {
  case attempt() {
    Done(value:) -> Answered(value:)
    Fail(error:) -> Failed(error:)
    Retry -> {
      let remaining = deadline - monotonic_ms()
      case remaining > 0 {
        False -> Expired
        True -> {
          process.sleep(int.min(every, remaining))
          loop(deadline, every, attempt)
        }
      }
    }
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
