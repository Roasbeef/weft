//// Tests for the run engine.
////
//// Each test is named for the invariant it defends rather than for the
//// function it calls, because the interesting claims here are properties of a
//// run — every task is accounted for, no worker outlives its scope, the bound
//// is a bound — and several of them are only observable from *outside* the
//// process that started the run. That is why so many of these spawn a harness
//// process and watch it from here: a test that calls `start` and then asserts
//// on the result cannot see a caller being killed mid-run.
////
//// Timing is kept coarse on purpose. Where a test needs "this task is still
//// running", the task sleeps for tens of seconds and the test kills it; where
//// it needs "this task finishes first", the gap between the fast and the slow
//// task is at least an order of magnitude. Nothing here waits on a sleep that
//// is meant to expire.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleeunit
import weft.{
  type Outcome, Abandoned, CancelSiblings, Completed, Continue, Crashed, Failed,
  Halt, KeepGoing, NeverStarted,
}

pub fn main() -> Nil {
  gleeunit.main()
}

// --- Every task is accounted for --------------------------------------------

pub fn an_empty_run_has_an_empty_account_test() {
  let account: List(Outcome(Int, String)) = weft.start(weft.new([]))
  assert account == []
}

pub fn every_task_yields_exactly_one_outcome_test() {
  let account =
    weft.new([
      fn() { Ok(1) },
      fn() { Error("no") },
      fn() { panic as "this task is supposed to crash" },
      fn() { Ok(4) },
    ])
    |> weft.start

  let assert [first, second, third, fourth] = account
    as "four tasks must produce four outcomes"
  assert first == Completed(0, 1)
  assert second == Failed(1, "no")
  assert fourth == Completed(3, 4)

  // The exit reason is carried rather than flattened, so a crash is still
  // matchable after the fact.
  let assert Crashed(2, process.Abnormal(_)) = third
    as "a panicking task crashes abnormally"
}

pub fn a_bounded_run_accounts_for_every_task_in_input_order_test() {
  let numbers = counting_up_to(25)
  let account =
    numbers
    |> list.map(fn(n) { fn() { Ok(n) } })
    |> weft.new
    |> weft.limit(3)
    |> weft.start

  assert list.map(account, fn(outcome) { outcome.index }) == numbers
  assert weft.values(account) == numbers
}

pub fn every_configuration_accounts_for_every_task_test() {
  // A mix of every way a task can end: succeed, fail, die, and take long
  // enough that some bounds leave it running when the run is cancelled.
  let tasks = [
    fn() { Ok(0) },
    fn() { Error("one") },
    self_destructing_task(),
    sleeper(30, 3),
    fn() { Ok(4) },
    fn() { Error("five") },
  ]
  let indices = counting_up_to(6)

  // The claim under test is the one D1 makes and the one every other feature
  // has to keep true: whatever the bound and whatever the failure policy, the
  // account has exactly one entry per task and no entry for anything else.
  list.each([1, 2, 3, 5, 8], fn(bound) {
    list.each([KeepGoing, CancelSiblings], fn(policy) {
      let account =
        weft.new(tasks)
        |> weft.limit(bound)
        |> weft.on_failure(policy)
        |> weft.start
      assert list.map(account, fn(outcome) { outcome.index }) == indices
    })
  })
}

pub fn start_restores_input_order_from_completion_order_test() {
  let account =
    weft.new([sleeper(120, 0), sleeper(20, 1), sleeper(60, 2)])
    |> weft.limit(3)
    |> weft.start

  assert account == [Completed(0, 0), Completed(1, 1), Completed(2, 2)]
}

pub fn cancel_siblings_accounts_for_the_tasks_it_cancels_test() {
  let account =
    weft.new([fn() { Error("first") }, sleeper(30_000, 1), sleeper(30_000, 2)])
    |> weft.limit(2)
    |> weft.on_failure(CancelSiblings)
    |> weft.start

  // Two slots, so task 1 was running when task 0 failed and task 2 had not
  // started. The distinction between the two is the whole reason `Abandoned`
  // and `NeverStarted` are separate variants.
  assert account == [Failed(0, "first"), Abandoned(1), NeverStarted(2)]
}

// --- Nothing outlives its owner ---------------------------------------------

pub fn no_worker_survives_a_killed_caller_test() {
  let reporter = process.new_subject()

  // The run has to happen somewhere we are willing to kill, so it happens in a
  // harness process that this test owns and this test destroys.
  let harness =
    process.spawn_unlinked(fn() {
      let account =
        weft.new(list.repeat(reporting_sleeper(reporter), 3))
        |> weft.limit(3)
        |> weft.start
      account_sink(account)
    })

  let workers = collect_pids(reporter, 3, [])
  assert list.all(workers, process.is_alive)

  process.kill(harness)

  // The caller's death reaches the scope as a trapped EXIT; the scope kills its
  // workers and waits for them before it exits. Nothing in that chain depends
  // on this test still being here.
  assert dead_within(workers, 300)
}

pub fn a_worker_killed_by_a_third_party_is_a_crash_test() {
  let reporter = process.new_subject()
  let finished = process.new_subject()

  let _harness =
    process.spawn_unlinked(fn() {
      let account =
        weft.new([reporting_sleeper(reporter), sleeper(200, 1)])
        |> weft.limit(2)
        |> weft.start
      process.send(finished, account)
    })

  let assert Ok(worker) = process.receive(reporter, 2000)
    as "the first task must report its pid"
  process.kill(worker)

  let assert Ok(account) = process.receive(finished, 5000)
    as "the run must survive a worker being killed under it"

  // The scope never asked for this death, so calling it `Abandoned` would claim
  // the run cancelled work it did not cancel.
  assert account == [Crashed(0, process.Killed), Completed(1, 1)]
}

// --- The bound is a bound ---------------------------------------------------

pub fn limit_bounds_how_many_tasks_run_at_once_test() {
  let counter = start_counter()
  let account =
    weft.new(list.repeat(counting_task(counter), 10))
    |> weft.limit(2)
    |> weft.start

  assert list.length(account) == 10
  assert peak_concurrency(counter) == 2
}

// --- Deadlines --------------------------------------------------------------

pub fn a_deadline_abandons_what_was_running_test() {
  let account =
    weft.new([fn() { Ok(0) }, sleeper(30_000, 1)])
    |> weft.limit(2)
    |> weft.deadline(80)
    |> weft.start

  assert account == [Completed(0, 0), Abandoned(1)]
}

pub fn a_deadline_never_starts_what_was_queued_test() {
  let account =
    weft.new([sleeper(30_000, 0), sleeper(30_000, 1)])
    |> weft.limit(1)
    |> weft.deadline(80)
    |> weft.start

  assert account == [Abandoned(0), NeverStarted(1)]
}

pub fn an_early_finish_leaves_no_stale_deadline_test() {
  let first =
    weft.new([fn() { Ok(0) }, fn() { Ok(1) }])
    |> weft.deadline(40)
    |> weft.start
  assert first == [Completed(0, 0), Completed(1, 1)]

  // Well past the point where the first run's timer would have fired. If the
  // scope had left it armed, the message would have to land somewhere, and this
  // process is the only place left for it to land.
  process.sleep(90)

  let second = weft.new([sleeper(30, 2)]) |> weft.start
  assert second == [Completed(0, 2)]
}

// --- Cancellation from outside ----------------------------------------------

pub fn a_cancel_signal_unblocks_a_caller_stuck_in_start_test() {
  let stop = weft.cancel_signal()
  let finished = process.new_subject()

  let _harness =
    process.spawn_unlinked(fn() {
      let account =
        weft.new(list.repeat(sleeper(30_000, 0), 3))
        |> weft.limit(2)
        |> weft.cancel_with(stop)
        |> weft.start
      process.send(finished, account)
    })

  // The harness is blocked inside `start` and cannot act on its own behalf,
  // which is exactly the case a handle-based `cancel(task)` cannot reach.
  process.sleep(60)
  weft.cancel(stop)

  let assert Ok(account) = process.receive(finished, 5000)
    as "firing the signal must end the run"
  assert account == [Abandoned(0), Abandoned(1), NeverStarted(2)]
}

pub fn a_spent_signal_starts_no_work_at_all_test() {
  let stop = weft.cancel_signal()
  weft.cancel(stop)
  process.sleep(20)

  let account =
    weft.new([sleeper(30_000, 0), sleeper(30_000, 1)])
    |> weft.cancel_with(stop)
    |> weft.start

  assert account == [NeverStarted(0), NeverStarted(1)]
}

// --- fold -------------------------------------------------------------------

pub fn fold_sees_outcomes_in_completion_order_test() {
  let seen =
    weft.new([sleeper(150, 0), sleeper(20, 1), sleeper(70, 2)])
    |> weft.limit(3)
    |> weft.fold(from: [], with: fn(seen, outcome) {
      Continue([outcome.index, ..seen])
    })

  assert list.reverse(seen) == [1, 2, 0]
}

pub fn fold_carries_an_accumulator_over_the_whole_account_test() {
  let successes =
    weft.new([fn() { Ok(1) }, fn() { Error("no") }, fn() { Ok(3) }])
    |> weft.fold(from: 0, with: fn(total, outcome) {
      case outcome {
        Completed(value:, ..) -> Continue(total + value)
        Failed(..) -> Continue(total)
        Crashed(..) -> Continue(total)
        Abandoned(..) -> Continue(total)
        NeverStarted(..) -> Continue(total)
      }
    })

  assert successes == 4
}

pub fn a_halted_fold_cancels_and_reaps_the_rest_test() {
  let reporter = process.new_subject()

  let consumed =
    weft.new([
      fn() { Ok(0) },
      reporting_sleeper(reporter),
      reporting_sleeper(reporter),
    ])
    |> weft.limit(3)
    |> weft.fold(from: [], with: fn(seen, outcome) { Halt([outcome, ..seen]) })

  // Halting returns the accumulator and nothing else: the outcomes of the
  // cancelled tasks have nowhere to go, which is documented and is why a caller
  // who wants the full account of a stopped run uses `start` with a signal.
  assert consumed == [Completed(0, 0)]

  // The teardown is not "eventually"; `fold` does not return until the scope
  // has killed and joined everything it spawned. These two were sleeping for
  // half a minute, so if the halt had not reaped them they would still be here.
  let workers = collect_pids(reporter, 2, [])
  assert dead_within(workers, 300)
}

// --- race and first_ok ------------------------------------------------------

pub fn race_returns_the_first_task_to_finish_test() {
  let outcome = weft.race(sleeper(200, 0), [sleeper(20, 1), sleeper(400, 2)])
  assert outcome == Completed(1, 1)
}

pub fn race_reports_a_failure_that_got_there_first_test() {
  // `race` is the first task to *complete*, not the first to succeed. Treating
  // these as the same function is a common bug, so the difference is tested.
  let outcome = weft.race(fn() { Error("fast") }, [sleeper(300, 1)])
  assert outcome == Failed(0, "fast")
}

pub fn race_kills_the_losers_test() {
  let reporter = process.new_subject()
  let outcome =
    weft.race(sleeper(60, 0), [
      reporting_sleeper(reporter),
      reporting_sleeper(reporter),
    ])

  assert outcome == Completed(0, 0)
  let losers = collect_pids(reporter, 2, [])
  assert dead_within(losers, 300)
}

pub fn first_ok_skips_a_failure_and_takes_a_later_success_test() {
  let answer =
    weft.first_ok([fn() { Error("no") }, sleeper(40, 7), sleeper(30_000, 9)])
  assert answer == Ok(7)
}

pub fn first_ok_reports_the_whole_account_when_nothing_succeeds_test() {
  let answer = weft.first_ok([fn() { Error("a") }, fn() { Error("b") }])
  assert answer == Error([Failed(0, "a"), Failed(1, "b")])
}

pub fn first_ok_of_nothing_is_an_empty_account_test() {
  let answer: Result(Int, List(Outcome(Int, String))) = weft.first_ok([])
  assert answer == Error([])
}

// --- Sugar and accessors ----------------------------------------------------

pub fn map_runs_the_function_over_every_item_test() {
  let account =
    weft.map([1, 2, 3], limit: 2, with: fn(n) {
      case n == 2 {
        True -> Error("two")
        False -> Ok(n * 10)
      }
    })

  assert account == [Completed(0, 10), Failed(1, "two"), Completed(2, 30)]
}

pub fn partition_keeps_order_and_preserves_everything_else_test() {
  let account = [
    Completed(0, "a"),
    Failed(1, "x"),
    Crashed(2, process.Killed),
    Abandoned(3),
    NeverStarted(4),
    Completed(5, "b"),
  ]

  assert weft.partition(account)
    == #(["a", "b"], [
      Failed(1, "x"),
      Crashed(2, process.Killed),
      Abandoned(3),
      NeverStarted(4),
    ])
  assert weft.values(account) == ["a", "b"]
  assert weft.failures(account) == ["x"]
}

pub fn partition_round_trips_a_real_account_test() {
  let account =
    weft.new([fn() { Ok(1) }, fn() { Error("no") }, fn() { Ok(3) }])
    |> weft.start
  let #(values, rest) = weft.partition(account)

  assert values == weft.values(account)
  assert list.length(values) + list.length(rest) == list.length(account)
  assert weft.failures(rest) == weft.failures(account)
}

// --- Tasks ------------------------------------------------------------------

/// A task that takes `milliseconds` and then succeeds with `value`. Used both
/// for "finishes second" and, with a long sleep, for "is definitely still
/// running when the test looks".
fn sleeper(milliseconds: Int, value: Int) -> fn() -> Result(Int, String) {
  fn() {
    process.sleep(milliseconds)
    Ok(value)
  }
}

/// A task that dies rather than returning.
///
/// It kills itself instead of panicking so that a test which builds many runs
/// out of it does not bury the report under one SASL crash dump per repetition;
/// the scope classifies it as `Crashed` either way, since no cancellation was
/// under way when it died.
fn self_destructing_task() -> fn() -> Result(Int, String) {
  fn() {
    process.kill(process.self())
    process.sleep(30_000)
    Ok(-1)
  }
}

/// A task that announces its own pid and then sleeps for far longer than any
/// test will wait. Announcing the pid is what lets a test outside the run check
/// that a worker really died rather than merely that the run returned.
fn reporting_sleeper(reporter: Subject(Pid)) -> fn() -> Result(Int, String) {
  fn() {
    process.send(reporter, process.self())
    process.sleep(30_000)
    Ok(-1)
  }
}

/// A task that reports itself to the concurrency counter for as long as it
/// runs.
fn counting_task(counter: Subject(Message)) -> fn() -> Result(Int, String) {
  fn() {
    process.send(counter, Enter)
    process.sleep(20)
    process.send(counter, Leave)
    Ok(0)
  }
}

// --- Watching from outside --------------------------------------------------

/// Gather `count` worker pids announced by `reporting_sleeper`.
fn collect_pids(
  from: Subject(Pid),
  count: Int,
  gathered: List(Pid),
) -> List(Pid) {
  case count <= 0 {
    True -> list.reverse(gathered)
    False -> {
      let assert Ok(pid) = process.receive(from, 2000)
        as "a worker should have announced itself"
      collect_pids(from, count - 1, [pid, ..gathered])
    }
  }
}

/// Poll until every pid is gone, or give up after `attempts` ten-millisecond
/// waits. Polling rather than monitoring keeps the check independent of the
/// mechanism under test: it asks the VM directly whether the process exists.
fn dead_within(pids: List(Pid), attempts: Int) -> Bool {
  case list.any(pids, process.is_alive) {
    False -> True
    True ->
      case attempts <= 0 {
        True -> False
        False -> {
          process.sleep(10)
          dead_within(pids, attempts - 1)
        }
      }
  }
}

/// Consume an account without asserting on it, so a harness process can run a
/// run purely for its side effects.
fn account_sink(account: List(Outcome(Int, String))) -> Nil {
  let _ignored = list.length(account)
  Nil
}

fn counting_up_to(total: Int) -> List(Int) {
  int.range(from: total, to: 0, with: [], run: fn(numbers, n) {
    [n - 1, ..numbers]
  })
}

// --- The concurrency counter ------------------------------------------------
//
// A tiny process that records how many tasks were inside their body at once.
// It is a process rather than a counter in the test because the tasks run in
// their own processes and there is nothing else they can safely share.

type Message {
  Enter
  Leave
  Peak(reply: Subject(Int))
}

fn start_counter() -> Subject(Message) {
  let ready = process.new_subject()
  let _counter =
    process.spawn_unlinked(fn() {
      // The inbox has to be created inside the counter process, because a
      // subject is owned by whoever made it, so the handshake is unavoidable.
      let inbox = process.new_subject()
      process.send(ready, inbox)
      counter_loop(inbox, 0, 0)
    })

  let assert Ok(inbox) = process.receive(ready, 2000)
    as "the counter must hand back the inbox it owns"
  inbox
}

fn counter_loop(inbox: Subject(Message), current: Int, peak: Int) -> Nil {
  case process.receive_forever(inbox) {
    Enter -> counter_loop(inbox, current + 1, int.max(peak, current + 1))
    Leave -> counter_loop(inbox, current - 1, peak)
    Peak(reply:) -> {
      process.send(reply, peak)
      counter_loop(inbox, current, peak)
    }
  }
}

fn peak_concurrency(counter: Subject(Message)) -> Int {
  process.call(counter, waiting: 2000, sending: Peak)
}
