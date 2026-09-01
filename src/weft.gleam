//// Owned, bounded structured concurrency.
////
//// A run is a list of tasks, a bound on how many of them may be in flight at
//// once, and a complete account of what happened to every one of them. The
//// account is the point: `start` hands back one `Outcome` per task, including
//// the tasks that crashed, the tasks that were cancelled mid flight, and the
//// tasks that never got a slot. There is no path through this module where a
//// late failure discards work that already succeeded.
////
//// ## Why there is an extra process
////
//// The guarantee this module exists to make is *no task outlives its scope,
//// and no scope outlives its caller*, and it is enforced by link propagation
//// rather than by a collector loop that has to stay alive to do its job. A
//// library that spawns unlinked workers and reaps them from a loop degrades to
//// "no work outlives the VM" the moment something kills that loop.
////
//// So every run pays for one extra process, the *scope*, and the link topology
//// does the work:
////
//// ```text
//// caller
////   |  link          (process.spawn, which is proc_lib:spawn_link)
////   v
//// scope              (traps exits)
////   |  link   link   link
////   v         v      v
////  w0        w1     w2         (workers, which do NOT trap)
//// ```
////
//// - The caller spawns the scope linked to itself.
//// - The scope calls `process.trap_exits(True)` and spawns every worker linked
////   to *itself*, from inside the scope process.
//// - A worker crashing therefore arrives at the scope as a trapped `EXIT` and
////   becomes a `Crashed` outcome. It does not take the scope down with it.
//// - The scope being killed — including by a brutal supervisor kill —
////   propagates down those links to workers that do not trap, so they die with
////   it. Nothing has to still be looping for that to hold.
//// - The caller dying sends an `EXIT` to the scope, which traps it, kills its
////   workers, waits for them, and exits.
////
//// One asymmetry in that picture is worth stating because it shaped the code:
//// a *normal* exit signal is ignored by a process that does not trap exits. So
//// the links alone do not reap workers when the scope finishes cleanly; on
//// every path where the scope stops early it kills its workers explicitly and
//// waits for their `EXIT`s before returning. `settled` is the predicate that
//// encodes it, and it counts workers that have already answered but not yet
//// exited (`finishing`) precisely so that the scope cannot return while one of
//// its children is still alive.
////
//// ## Kill then join, in that order
////
//// Cancellation sends *every* kill first and only then waits for the `EXIT`s.
//// Interleaving the two lets one slow exit postpone every later worker's
//// cancellation, which turns a bounded teardown into a serial one. `begin_cancel`
//// does the sending; the ordinary receive loop does the joining.
////
//// ## Delivery is pull, not push
////
//// `fold` consumes outcomes in completion order, and the streaming claim would
//// be a lie if the scope pushed each outcome at the caller as it landed: a slow
//// reducer would make the caller's mailbox the buffer, and mailboxes are
//// unbounded. Instead the scope holds outcomes and sends one only when the
//// caller has asked for it. The caller replies `Next` after each outcome, so at
//// most one outcome is ever in flight, and every `Delivered` message carries the
//// scope's own inbox so no handshake is needed to bootstrap the conversation.
////
//// Back pressure reaches the workers because a slot is occupied from the moment
//// a task is spawned until its outcome has been *delivered*, not until the task
//// returns. So `limit` bounds work in flight plus completed-but-unconsumed
//// results together, and a reducer that stops reading stops the run from
//// starting new work. The one place this bound is deliberately relaxed is
//// cancellation, which materialises one `NeverStarted` per unstarted task in a
//// single burst: those are three-word records and the task list was already a
//// materialised list, so the cost is one that the caller had already paid.
////
//// ## Cancellation, and how `Abandoned` stays honest
////
//// A run can be stopped from three directions: `on_failure(CancelSiblings)`
//// when a task fails, `deadline` when the wall clock runs out, and
//// `cancel_with` when some *other* process decides to end a run the caller is
//// blocked inside. All three converge on `begin_cancel`.
////
//// The scope cancels a worker with `process.kill`, so a cancelled worker's exit
//// reason is `Killed` — and so is the exit reason of a worker some unrelated
//// process killed. Those two are different facts and the module keeps them
//// apart with one boolean: a `Killed` exit *after* the scope initiated
//// cancellation is `Abandoned`; a `Killed` exit *before* it is
//// `Crashed(Killed)`, because nobody in this run asked for it.
////
//// ## What the caller may rely on
////
//// When `start` returns, every task has an outcome and no worker from that run
//// is alive. When `fold` returns — including when the reducer halted it early —
//// the scope has killed and joined everything it spawned before replying, so
//// the same holds. A caller that traps exits does not see the scope's normal
//// exit as mailbox noise, because the scope drops the link itself just before
//// it returns; the link is live for the whole run, which is the part that
//// matters.
////
//// ## Quick start
////
//// ```gleam
//// import weft
////
//// pub fn fetch_all(urls: List(String)) {
////   let outcomes =
////     urls
////     |> list.map(fn(url) { fn() { fetch(url) } })
////     |> weft.new
////     |> weft.limit(8)
////     |> weft.on_failure(weft.KeepGoing)
////     |> weft.deadline(30_000)
////     |> weft.start
////
////   // Every url is accounted for: bodies for the fetches that succeeded,
////   // and one Outcome per fetch that failed, crashed, or ran out of time.
////   let #(bodies, rest) = weft.partition(outcomes)
////   #(bodies, rest)
//// }
//// ```

import gleam/bool
import gleam/erlang/process.{type ExitReason, type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set

// --- The account ------------------------------------------------------------

/// What happened to exactly one task.
///
/// Every task in a run produces exactly one of these, so a run is always fully
/// accounted for and never collapses into a single `Result` that throws away
/// the tasks which did succeed. The `index` is the task's position in the list
/// given to `new`, which is what lets `start` restore input order and what lets
/// a caller match an outcome back to the work that produced it.
pub type Outcome(a, e) {
  /// The task returned `Ok`.
  Completed(
    /// The task's position in the list given to `new`, counting from zero.
    index: Int,
    /// The value the task returned.
    value: a,
  )
  /// The task returned `Error`. The task itself decided this; nothing went
  /// wrong with the run.
  Failed(
    /// The task's position in the list given to `new`, counting from zero.
    index: Int,
    /// The error the task returned.
    error: e,
  )
  /// The task's process died instead of returning. The Erlang exit reason is
  /// carried rather than flattened to a string, so a caller can match on it.
  Crashed(
    /// The task's position in the list given to `new`, counting from zero.
    index: Int,
    /// Why the worker process exited.
    reason: ExitReason,
  )
  /// The task was started, then cancelled before it finished. Work was done
  /// and thrown away.
  Abandoned(
    /// The task's position in the list given to `new`, counting from zero.
    index: Int,
  )
  /// The run ended before this task ever got a slot. No work was done.
  NeverStarted(
    /// The task's position in the list given to `new`, counting from zero.
    index: Int,
  )
}

/// What a run does to the remaining tasks when one of them fails.
pub type OnFailure {
  /// Run everything to completion and report every outcome. The default.
  KeepGoing
  /// The first `Failed` or `Crashed` outcome cancels the rest: running tasks
  /// become `Abandoned` and unstarted ones become `NeverStarted`.
  CancelSiblings
}

/// A reducer's verdict on whether to keep consuming outcomes.
///
/// This is `fold`'s answer to the problem a plain `fn(acc, outcome) -> acc`
/// cannot express: a reducer that has seen enough has no way to say so, and the
/// caller is left reaching for an external cancel signal to make a decision the
/// reducer already made.
pub type Step(acc) {
  /// Keep going: ask the scope for the next outcome.
  Continue(
    /// The accumulator to carry into the next outcome.
    accumulator: acc,
  )
  /// Stop here: the remaining tasks are cancelled and the run is torn down
  /// before `fold` returns.
  Halt(
    /// The accumulator `fold` will return.
    accumulator: acc,
  )
}

// --- The cancel signal ------------------------------------------------------

/// An external stop signal. Any process holding one can end a run that another
/// process is blocked inside, which is the case a handle-based `cancel(task)`
/// cannot reach: the caller is the thing that is blocked.
///
/// A signal is a tiny process that does nothing but stay alive. `cancel` kills
/// it, and every scope watching it sees the death. That is why one signal can
/// serve several runs at once, and why a signal is **one-shot**: a killed
/// process cannot come back, so a `Cancel` that has fired stays fired. Handing a
/// spent signal to `cancel_with` cancels that run immediately, before any task
/// starts.
pub opaque type Cancel {
  Cancel(
    /// The signal process. Its death is the signal.
    pid: Pid,
  )
}

/// Create a fresh cancel signal.
///
/// The returned value can be passed to `cancel_with` on any number of runs and
/// shared with any number of processes. It costs one idle process, which lives
/// until `cancel` is called on it.
///
/// ## Examples
///
/// ```gleam
/// let stop = weft.cancel_signal()
///
/// let outcomes =
///   weft.new(tasks)
///   |> weft.cancel_with(stop)
///   |> weft.start
/// ```
pub fn cancel_signal() -> Cancel {
  // Unlinked deliberately: the signal outlives the process that created it and
  // must not take anybody down when it is fired.
  Cancel(pid: process.spawn_unlinked(fn() { process.sleep_forever() }))
}

/// Fire a cancel signal, ending every run watching it.
///
/// Firing a signal that has already fired does nothing. Runs watching the
/// signal report `Abandoned` for the tasks that were running and `NeverStarted`
/// for the tasks that had not begun.
///
/// ## Examples
///
/// ```gleam
/// let stop = weft.cancel_signal()
/// process.spawn(fn() { weft.new(tasks) |> weft.cancel_with(stop) |> weft.start })
///
/// // From any process, at any time, including while the run's caller is
/// // blocked inside `start`:
/// weft.cancel(stop)
/// ```
pub fn cancel(signal: Cancel) -> Nil {
  process.kill(signal.pid)
}

// --- The builder ------------------------------------------------------------

/// A configured run, not yet started.
///
/// Built with `new` and refined with `limit`, `on_failure`, `cancel_with` and
/// `deadline`; `start` and `fold` are the terminal verbs. A `Run` is an
/// ordinary immutable value, so the same one can be started more than once.
pub opaque type Run(a, e) {
  Run(
    tasks: List(fn() -> Result(a, e)),
    limit: Int,
    on_failure: OnFailure,
    signal: Option(Cancel),
    within: Option(Int),
  )
}

/// Begin a run over `tasks`, bounded by default.
///
/// The default limit is the number of schedulers online, not unbounded:
/// spawning one process per item is fine for pure computation and actively
/// harmful for anything holding a socket, a file handle, a database connection
/// or a rate-limited quota. Someone who wants a wider fan-out says so with
/// `limit`.
///
/// The default failure policy is `KeepGoing`.
///
/// ## Examples
///
/// ```gleam
/// let outcomes =
///   urls
///   |> list.map(fn(url) { fn() { fetch(url) } })
///   |> weft.new
///   |> weft.start
/// ```
pub fn new(tasks: List(fn() -> Result(a, e))) -> Run(a, e) {
  Run(
    tasks:,
    limit: default_limit(),
    on_failure: KeepGoing,
    signal: None,
    within: None,
  )
}

/// Set how many tasks may occupy a slot at once.
///
/// A slot is held from the moment a task is spawned until its outcome has been
/// handed to the consumer, so this bounds running work and completed-but-
/// unconsumed results together. A `max` below one is raised to one; there is no
/// unbounded setting, by design.
///
/// ## Examples
///
/// ```gleam
/// let outcomes =
///   weft.new(tasks)
///   |> weft.limit(8)
///   |> weft.start
/// ```
pub fn limit(run: Run(a, e), max: Int) -> Run(a, e) {
  let limit = int.max(1, max)
  Run(..run, limit:)
}

/// Set what a failing task does to the tasks beside it.
///
/// ## Examples
///
/// ```gleam
/// // Stop at the first `Failed` or `Crashed`; the rest are reported as
/// // `Abandoned` or `NeverStarted` rather than being lost.
/// let outcomes =
///   weft.new(tasks)
///   |> weft.on_failure(weft.CancelSiblings)
///   |> weft.start
/// ```
pub fn on_failure(run: Run(a, e), policy: OnFailure) -> Run(a, e) {
  Run(..run, on_failure: policy)
}

/// Watch a cancel signal for the duration of the run.
///
/// This is the only way to stop a run from outside the process that started it,
/// because that process is blocked inside `start` or `fold` and cannot act on
/// its own behalf.
///
/// ## Examples
///
/// ```gleam
/// let stop = weft.cancel_signal()
///
/// let outcomes =
///   weft.new(tasks)
///   |> weft.cancel_with(stop)
///   |> weft.start
/// ```
pub fn cancel_with(run: Run(a, e), signal: Cancel) -> Run(a, e) {
  Run(..run, signal: Some(signal))
}

/// Give the whole run a wall-clock budget, in milliseconds.
///
/// Hitting the deadline is not an error, it is a reason some entries in the
/// account are `Abandoned` (started, then cut short) or `NeverStarted` (never
/// got a slot). The timer is cancelled and flushed if the run finishes first,
/// so it can never fire into a later receive.
///
/// ## Examples
///
/// ```gleam
/// let outcomes =
///   weft.new(tasks)
///   |> weft.deadline(30_000)
///   |> weft.start
/// ```
pub fn deadline(run: Run(a, e), within: Int) -> Run(a, e) {
  Run(..run, within: Some(int.max(0, within)))
}

// --- Running ----------------------------------------------------------------

/// Run everything to completion and return every outcome in **input** order.
///
/// Blocks the calling process until the run is over. When it returns, every
/// task has exactly one outcome and no worker from the run is alive.
///
/// Ordered versus unordered falls out of which terminal verb you call rather
/// than out of a flag: `start` sorts the account back into input order, `fold`
/// hands outcomes over in completion order as they land.
///
/// ## Examples
///
/// ```gleam
/// let outcomes =
///   weft.new([fn() { Ok(1) }, fn() { Error("no") }])
///   |> weft.start
/// // -> [weft.Completed(0, 1), weft.Failed(1, "no")]
/// ```
///
/// ```gleam
/// let #(values, rest) =
///   weft.new(tasks)
///   |> weft.limit(4)
///   |> weft.on_failure(weft.CancelSiblings)
///   |> weft.start
///   |> weft.partition
/// ```
pub fn start(run: Run(a, e)) -> List(Outcome(a, e)) {
  let total = list.length(run.tasks)
  let #(gathered, ending) =
    drive(run, [], fn(account, outcome) { Continue([outcome, ..account]) })

  // A scope that dies without saying `Done` is outside this module's contract:
  // it means something killed the scope from outside the run. The account still
  // has to be total, so the tasks we never heard about are reported as having
  // died the way the scope did.
  let outcomes = case ending {
    None -> gathered
    Some(reason) -> list.append(gathered, unaccounted(gathered, total, reason))
  }
  by_index(outcomes)
}

/// Consume outcomes in **completion** order as they land.
///
/// This is how a run whose results do not all fit in memory is processed, and
/// how a caller acts on early results without waiting for the slow tail. The
/// scope holds each outcome until the reducer asks for it, so at most one is
/// ever in flight and a slow reducer stops new work from starting rather than
/// filling a mailbox.
///
/// A reducer returning `Halt` ends the run: the remaining tasks are killed and
/// joined before `fold` returns, so no worker survives the call. The outcomes
/// of those tasks are *not* delivered — `fold` returns an accumulator and has
/// nowhere to put them — so a caller that wants the full account of a run it
/// stopped early should use `start` with a cancel signal instead.
///
/// ## Examples
///
/// ```gleam
/// // Count successes without holding every value.
/// let successes =
///   weft.new(tasks)
///   |> weft.fold(from: 0, with: fn(count, outcome) {
///     case outcome {
///       weft.Completed(..) -> weft.Continue(count + 1)
///       weft.Failed(..) | weft.Crashed(..) -> weft.Continue(count)
///       weft.Abandoned(..) | weft.NeverStarted(..) -> weft.Continue(count)
///     }
///   })
/// ```
///
/// ```gleam
/// // Stop as soon as three values are in hand; the rest are cancelled.
/// let first_three =
///   weft.new(tasks)
///   |> weft.fold(from: [], with: fn(found, outcome) {
///     let found = case outcome {
///       weft.Completed(value:, ..) -> [value, ..found]
///       weft.Failed(..) | weft.Crashed(..) -> found
///       weft.Abandoned(..) | weft.NeverStarted(..) -> found
///     }
///     case list.length(found) >= 3 {
///       True -> weft.Halt(found)
///       False -> weft.Continue(found)
///     }
///   })
/// ```
pub fn fold(
  run: Run(a, e),
  from initial: acc,
  with reducer: fn(acc, Outcome(a, e)) -> Step(acc),
) -> acc {
  let #(accumulator, _ending) = drive(run, initial, reducer)
  accumulator
}

// --- Sugar ------------------------------------------------------------------

/// Apply a fallible function to every item, bounded, and return the account.
///
/// Exactly `weft.new` over the items with `limit` and `start`, which is the
/// shape most callers want and the one it is easiest to get wrong by mapping an
/// unbounded `async` over a list.
///
/// ## Examples
///
/// ```gleam
/// let outcomes = weft.map(urls, limit: 8, with: fetch)
/// let #(bodies, rest) = weft.partition(outcomes)
/// ```
pub fn map(
  items: List(a),
  limit max: Int,
  with fun: fn(a) -> Result(b, e),
) -> List(Outcome(b, e)) {
  items
  |> list.map(fn(item) { fn() { fun(item) } })
  |> new
  |> limit(max)
  |> start
}

/// Run tasks concurrently and return the first one to *complete*, however it
/// completed. The losers are killed and joined before this returns.
///
/// The first task is a separate argument from the rest because a race over
/// nothing has no answer: `Outcome` has no variant meaning "there was nothing to
/// race", and wrapping the result in a `Result` would push an error case onto
/// every call site that already knows its list is not empty. Making the empty
/// case unrepresentable is cheaper than making every caller handle it. A caller
/// holding a runtime list matches on it once and decides for itself what
/// nothing should mean.
///
/// Note the difference from `first_ok`: this returns the first task to
/// *finish*, even if it finished by failing or crashing. Conflating the two is a
/// common bug, which is why they are separate functions.
///
/// ## Examples
///
/// ```gleam
/// // Whichever mirror answers first, even if it answers with an error.
/// let outcome = weft.race(fn() { fetch(mirror_a) }, [fn() { fetch(mirror_b) }])
/// // -> weft.Completed(index: 1, value: <-body->)
/// ```
pub fn race(
  first: fn() -> Result(a, e),
  rest: List(fn() -> Result(a, e)),
) -> Outcome(a, e) {
  let tasks = [first, ..rest]
  let run = new(tasks) |> limit(list.length(tasks))

  // Halting on the very first outcome is what cancels the losers: the scope
  // kills and joins them before it answers, so no separate cancel-on-success
  // policy is needed inside the engine.
  let #(winner, ending) =
    drive(run, None, fn(_, outcome) { Halt(Some(outcome)) })

  case winner {
    Some(outcome) -> outcome
    // Unreachable while the scope is alive, since a run of at least one task
    // always yields an outcome before it says `Done`. It is reachable if
    // something outside the run kills the scope, and then the honest report is
    // that the first task died the way the scope did.
    None -> Crashed(index: 0, reason: option.unwrap(ending, process.Normal))
  }
}

/// Run tasks concurrently and return the first one to *succeed*, falling back
/// to the full account if none did. The losers are killed and joined before a
/// success returns.
///
/// A `Failed` or `Crashed` task does not end the run here; that is the whole
/// point of the function, and it is why `first_ok` leaves `on_failure` at
/// `KeepGoing`. An empty list is answerable, unlike in `race`, because the error
/// channel already exists: it yields `Error([])`.
///
/// If something outside the run destroys the scope, the `Error` carries only
/// the outcomes heard before the death rather than a total account; reach for
/// `start`, which fills the gap in, when that distinction matters.
///
/// ## Examples
///
/// ```gleam
/// let body = weft.first_ok([fn() { fetch(mirror_a) }, fn() { fetch(mirror_b) }])
/// // -> Ok(<-body->)
/// ```
///
/// ```gleam
/// // Nothing succeeded, so the caller gets the whole account in input order.
/// let answer = weft.first_ok([fn() { Error("a") }, fn() { Error("b") }])
/// // -> Error([weft.Failed(0, "a"), weft.Failed(1, "b")])
/// ```
pub fn first_ok(
  tasks: List(fn() -> Result(a, e)),
) -> Result(a, List(Outcome(a, e))) {
  let run = new(tasks) |> limit(int.max(1, list.length(tasks)))
  let #(account, _ending) =
    drive(run, [], fn(account, outcome) {
      let account = [outcome, ..account]
      case outcome {
        Completed(..) -> Halt(account)
        Failed(..) -> Continue(account)
        Crashed(..) -> Continue(account)
        Abandoned(..) -> Continue(account)
        NeverStarted(..) -> Continue(account)
      }
    })

  // The success, if there is one, is the outcome the reducer halted on, so it
  // is at the head; the scan is over a list that is one element long in the
  // happy case.
  account
  |> list.find_map(fn(outcome) {
    case outcome {
      Completed(value:, ..) -> Ok(value)
      Failed(..) -> Error(Nil)
      Crashed(..) -> Error(Nil)
      Abandoned(..) -> Error(Nil)
      NeverStarted(..) -> Error(Nil)
    }
  })
  |> result.map_error(fn(_) { by_index(account) })
}

// --- Reading an account -----------------------------------------------------

/// Split an account into the values that succeeded and everything else.
///
/// Both halves keep the order they were given in. The second half stays a list
/// of `Outcome` rather than a list of errors, because "did not succeed" covers
/// four different facts and flattening them is how the accounting gets thrown
/// away one call site at a time.
///
/// ## Examples
///
/// ```gleam
/// let outcomes = [weft.Completed(0, "a"), weft.Failed(1, "no")]
/// assert weft.partition(outcomes) == #(["a"], [weft.Failed(1, "no")])
/// ```
pub fn partition(
  outcomes: List(Outcome(a, e)),
) -> #(List(a), List(Outcome(a, e))) {
  let #(succeeded, rest) =
    list.fold(outcomes, #([], []), fn(split, outcome) {
      let #(succeeded, rest) = split
      case outcome {
        Completed(value:, ..) -> #([value, ..succeeded], rest)
        Failed(..) -> #(succeeded, [outcome, ..rest])
        Crashed(..) -> #(succeeded, [outcome, ..rest])
        Abandoned(..) -> #(succeeded, [outcome, ..rest])
        NeverStarted(..) -> #(succeeded, [outcome, ..rest])
      }
    })
  #(list.reverse(succeeded), list.reverse(rest))
}

/// The values of the tasks that returned `Ok`, in the order given.
///
/// ## Examples
///
/// ```gleam
/// let outcomes = [weft.Completed(0, 1), weft.Abandoned(1), weft.Completed(2, 3)]
/// assert weft.values(outcomes) == [1, 3]
/// ```
pub fn values(outcomes: List(Outcome(a, e))) -> List(a) {
  list.filter_map(outcomes, fn(outcome) {
    case outcome {
      Completed(value:, ..) -> Ok(value)
      Failed(..) -> Error(Nil)
      Crashed(..) -> Error(Nil)
      Abandoned(..) -> Error(Nil)
      NeverStarted(..) -> Error(Nil)
    }
  })
}

/// The errors of the tasks that returned `Error`, in the order given.
///
/// Tasks that crashed, were abandoned, or never started are not errors the
/// tasks chose to return, so they appear here in no form at all; reach for
/// `partition` when you need to see them.
///
/// ## Examples
///
/// ```gleam
/// let outcomes = [weft.Failed(0, "a"), weft.Crashed(1, process.Killed)]
/// assert weft.failures(outcomes) == ["a"]
/// ```
pub fn failures(outcomes: List(Outcome(a, e))) -> List(e) {
  list.filter_map(outcomes, fn(outcome) {
    case outcome {
      Failed(error:, ..) -> Ok(error)
      Completed(..) -> Error(Nil)
      Crashed(..) -> Error(Nil)
      Abandoned(..) -> Error(Nil)
      NeverStarted(..) -> Error(Nil)
    }
  })
}

// --- The caller's half of the protocol --------------------------------------
//
// Everything below here is private. The caller spawns the scope, then does
// nothing but consume: it never spawns a worker, never sees an EXIT that is not
// its own concern, and never decides when to cancel. That asymmetry is what
// makes the ownership story checkable — there is exactly one process that knows
// which workers exist.

/// What the scope says to the caller. Every delivery carries the scope's inbox,
/// which is what removes the need for a separate handshake message: the caller
/// cannot reply before it has been spoken to, and it never needs to.
type Reply(a, e) {
  Delivered(inbox: Subject(Request), outcome: Outcome(a, e))
  Done
}

/// What the caller says back. One of these follows every `Delivered`, and
/// nothing else is ever sent.
type Request {
  /// The outcome has been consumed; send the next one when there is one.
  Next
  /// The reducer halted. Cancel the rest and tell me when the teardown is done.
  Stop
}

/// What the caller's receive can produce. The monitor arm exists so that a
/// caller which traps exits cannot hang forever if something destroys the scope
/// from outside the run; without it, the caller's only notification would be the
/// link, which a trapping caller turns into a message it is not waiting for.
type CallerEvent(a, e) {
  FromScope(reply: Reply(a, e))
  ScopeDown(reason: ExitReason)
}

/// Spawn a scope for `run`, consume its outcomes with `reducer`, and report both
/// the accumulator and whether the scope died instead of finishing.
///
/// The `Option(ExitReason)` is the seam `start` needs to keep its account total
/// in a situation this module does not otherwise have a story for.
fn drive(
  run: Run(a, e),
  initial: acc,
  reducer: fn(acc, Outcome(a, e)) -> Step(acc),
) -> #(acc, Option(ExitReason)) {
  let Run(tasks:, limit:, on_failure:, signal:, within:) = run
  let indexed = list.index_map(tasks, fn(task, index) { #(index, task) })
  let outbox = process.new_subject()
  let caller = process.self()

  // proc_lib:spawn_link, so the link is in place before the scope runs its
  // first instruction. A caller that dies during the handshake is therefore
  // still a caller the scope will hear about.
  let scope =
    process.spawn(fn() {
      run_scope(caller, outbox, indexed, limit, on_failure, signal, within)
    })

  let watch = process.monitor(scope)
  let replies =
    process.new_selector()
    |> process.select_map(outbox, FromScope)
    |> process.select_specific_monitor(watch, scope_down)

  let answer = consume(replies, initial, reducer)

  // Demonitoring flushes: a `DOWN` for a scope that has already finished must
  // not be left behind for whatever this process receives next.
  process.demonitor_process(watch)
  answer
}

fn scope_down(down: process.Down) -> CallerEvent(a, e) {
  case down {
    process.ProcessDown(reason:, ..) -> ScopeDown(reason:)
    process.PortDown(reason:, ..) -> ScopeDown(reason:)
  }
}

/// The caller's loop: one outcome at a time, one reply per outcome.
fn consume(
  replies: process.Selector(CallerEvent(a, e)),
  accumulator: acc,
  reducer: fn(acc, Outcome(a, e)) -> Step(acc),
) -> #(acc, Option(ExitReason)) {
  case process.selector_receive_forever(replies) {
    ScopeDown(reason:) -> #(accumulator, Some(reason))
    FromScope(Done) -> #(accumulator, None)
    FromScope(Delivered(inbox:, outcome:)) ->
      case reducer(accumulator, outcome) {
        Continue(accumulator:) -> {
          // Asking for the next outcome is also what frees the finished task's
          // slot, so a reducer that takes its time throttles the run rather
          // than filling this mailbox.
          process.send(inbox, Next)
          consume(replies, accumulator, reducer)
        }
        Halt(accumulator:) -> {
          process.send(inbox, Stop)
          #(accumulator, await_done(replies))
        }
      }
  }
}

/// Wait for the scope to finish tearing itself down after a `Stop`.
///
/// This is what makes "no worker survives a halted fold" true rather than
/// hopeful: the scope sends `Done` only once every worker it spawned has been
/// killed and reaped, so returning from here means returning to a caller with no
/// live descendants from this run.
fn await_done(
  replies: process.Selector(CallerEvent(a, e)),
) -> Option(ExitReason) {
  case process.selector_receive_forever(replies) {
    ScopeDown(reason:) -> Some(reason)
    FromScope(Done) -> None
    // The scope only delivers against outstanding demand and we withdrew ours
    // with `Stop`, so this arm is here for totality rather than for a race we
    // have seen. Dropping the outcome is right: the reducer said it was done.
    FromScope(Delivered(..)) -> await_done(replies)
  }
}

// --- The scope --------------------------------------------------------------

/// A worker's answer. The pid rides along because the scope tracks slots by pid
/// — it is the only identity an `EXIT` gives it — and the index rides along so
/// the outcome can be built without a lookup that might fail.
type Report(a, e) {
  Report(worker: Pid, index: Int, result: Result(a, e))
}

/// Everything the scope's single receive can produce, in one type, so the loop
/// is one flat dispatch rather than a stack of selective receives.
type Event(a, e) {
  Reported(report: Report(a, e))
  Exited(pid: Pid, reason: ExitReason)
  Asked(request: Request)
  DeadlinePassed
  SignalLost
}

/// Where the caller is in the conversation. This is a four-state machine rather
/// than a pair of booleans because the states are not independent: only
/// `Waiting` may be delivered to, and `Halted` and `Gone` differ solely in
/// whether anyone is left to hear the final `Done`.
type Consumer {
  /// Blocked on the next outcome. The scope may deliver.
  Waiting
  /// Holding an outcome and running the reducer. The scope must not deliver.
  Busy
  /// Asked to stop, and waiting only for the teardown to finish.
  Halted
  /// Dead. Nothing to report to, and nothing left to do but reap workers.
  Gone
}

/// The scope's whole state.
///
/// Two fields carry the invariants that are easiest to break. `finishing` holds
/// workers that have answered but not yet exited, and the scope refuses to
/// return while it is non-empty, because a scope that outran its children would
/// leave tasks alive with nothing owning them. `cancelling` is the boolean that
/// separates `Abandoned` from `Crashed(Killed)`: it is `True` exactly when this
/// scope is the one that sent the kills.
type Scope(a, e) {
  Scope(
    caller: Pid,
    outbox: Subject(Reply(a, e)),
    inbox: Subject(Request),
    reports: Subject(Report(a, e)),
    alarm: Subject(Nil),
    events: process.Selector(Event(a, e)),
    on_failure: OnFailure,
    timer: Option(process.Timer),
    /// Tasks that have not been spawned, in input order.
    pending: List(#(Int, fn() -> Result(a, e))),
    /// Spawned and unaccounted, keyed by pid because that is what an `EXIT`
    /// gives us.
    running: List(#(Pid, Int)),
    /// Accounted but not yet reaped. Their only remaining act is to exit.
    finishing: List(Pid),
    /// Outcomes waiting for the consumer to ask, in completion order.
    ready: Backlog(a, e),
    /// Free slots. Taken at spawn, returned at delivery. Not consulted once
    /// `cancelling` is `True`, since nothing new is spawned after that.
    slots: Int,
    consumer: Consumer,
    cancelling: Bool,
  )
}

/// The scope process, from `trap_exits` to its last `EXIT`.
fn run_scope(
  caller: Pid,
  outbox: Subject(Reply(a, e)),
  tasks: List(#(Int, fn() -> Result(a, e))),
  limit: Int,
  on_failure: OnFailure,
  signal: Option(Cancel),
  within: Option(Int),
) -> Nil {
  // Trapping first is what turns a worker crash into a `Crashed` outcome
  // instead of a dead scope, and what turns the caller's death into an event
  // this process can act on rather than a signal that kills it mid-teardown.
  process.trap_exits(True)

  let inbox = process.new_subject()
  let reports = process.new_subject()
  let alarm = process.new_subject()

  let timer = case within {
    None -> None
    Some(milliseconds) -> Some(process.send_after(alarm, milliseconds, Nil))
  }

  let events =
    process.new_selector()
    |> process.select_map(reports, Reported)
    |> process.select_map(inbox, Asked)
    |> process.select_map(alarm, fn(_) { DeadlinePassed })
    |> process.select_trapped_exits(fn(exit: process.ExitMessage) {
      Exited(pid: exit.pid, reason: exit.reason)
    })
    |> process.select_monitors(fn(_) { SignalLost })

  let scope =
    Scope(
      caller:,
      outbox:,
      inbox:,
      reports:,
      alarm:,
      events:,
      on_failure:,
      timer:,
      pending: tasks,
      running: [],
      finishing: [],
      ready: new_backlog(),
      slots: limit,
      // The caller is blocked on its first receive before this process runs at
      // all, so the run starts with one unit of demand already granted.
      consumer: Waiting,
      cancelling: False,
    )

  loop(watch_signal(scope, signal))
}

/// Watch the cancel signal, and settle the one question the monitor alone
/// cannot answer: whether the signal was already spent before the run began.
///
/// Monitoring rather than linking keeps the signal's death one-directional —
/// firing a signal must end runs, never kill the processes watching them. But a
/// `DOWN` for an already-dead pid arrives as a message, and `fill_slots` runs
/// before the scope's first receive, so a spent signal would otherwise spawn
/// every task only to kill it. Checking liveness here is the difference between
/// a run that reports `NeverStarted` for everything and one that does the work
/// twice over. The monitor still covers the pid that dies between these two
/// lines.
fn watch_signal(scope: Scope(a, e), signal: Option(Cancel)) -> Scope(a, e) {
  case signal {
    None -> scope
    Some(Cancel(pid:)) -> {
      let _watch = process.monitor(pid)
      case process.is_alive(pid) {
        True -> scope
        False -> begin_cancel(scope)
      }
    }
  }
}

/// Do everything that needs no message, then decide whether there is anything
/// left to wait for.
fn loop(scope: Scope(a, e)) -> Nil {
  let scope = deliver(fill_slots(scope))
  case settled(scope) {
    True -> finish(scope)
    False -> loop(step(scope, process.selector_receive_forever(scope.events)))
  }
}

/// Is there any work, any unreaped worker, or any undelivered outcome left?
///
/// The `finishing` clause is the one that is easy to drop and expensive to lose:
/// without it the scope could return while a worker that had already answered
/// was still alive, which is exactly the guarantee this module sells.
fn settled(scope: Scope(a, e)) -> Bool {
  list.is_empty(scope.pending)
  && list.is_empty(scope.running)
  && list.is_empty(scope.finishing)
  && backlog_is_empty(scope.ready)
}

/// Spawn into every free slot, in input order, until the bound or the queue
/// runs out.
fn fill_slots(scope: Scope(a, e)) -> Scope(a, e) {
  use <- bool.guard(when: scope.cancelling, return: scope)
  use <- bool.guard(when: scope.slots <= 0, return: scope)
  case scope.pending {
    [] -> scope
    [#(index, task), ..rest] -> {
      let worker = spawn_worker(scope.reports, index, task)
      fill_slots(
        Scope(..scope, pending: rest, slots: scope.slots - 1, running: [
          #(worker, index),
          ..scope.running
        ]),
      )
    }
  }
}

/// Spawn one worker, linked to this scope.
///
/// `process.spawn` is `proc_lib:spawn_link`, and it is being called from inside
/// the scope, so the new link is scope-to-worker. The worker does not trap, so
/// a kill or an abnormal exit from the scope reaches it as death rather than as
/// a message it would have to be alive to read.
fn spawn_worker(
  reports: Subject(Report(a, e)),
  index: Int,
  task: fn() -> Result(a, e),
) -> Pid {
  process.spawn(fn() {
    // The report is delivered before this process's own EXIT, because messages
    // and signals from one process to another keep their order. The scope leans
    // on that to tell an answered worker from a crashed one: a pid still in
    // `running` when its EXIT arrives never answered.
    process.send(
      reports,
      Report(worker: process.self(), index:, result: task()),
    )
  })
}

/// Hand one outcome to a caller that has asked for one.
fn deliver(scope: Scope(a, e)) -> Scope(a, e) {
  use <- bool.guard(when: scope.consumer != Waiting, return: scope)
  case pop_backlog(scope.ready) {
    Error(Nil) -> scope
    Ok(#(outcome, ready)) -> {
      // The slot returns here rather than when the task finished: that is what
      // makes `limit` bound completed-but-unconsumed results as well as work.
      process.send(scope.outbox, Delivered(inbox: scope.inbox, outcome:))
      Scope(..scope, ready:, slots: scope.slots + 1, consumer: Busy)
    }
  }
}

/// One event, one transition.
fn step(scope: Scope(a, e), event: Event(a, e)) -> Scope(a, e) {
  case event {
    Reported(report:) -> note_report(scope, report)
    Exited(pid:, reason:) -> note_exit(scope, pid, reason)
    Asked(request: Next) -> Scope(..scope, consumer: Waiting)
    Asked(request: Stop) -> detach(scope, Halted)
    // Clearing the timer here is half of the flush: a timer that has fired
    // cannot be cancelled, and `flush_deadline` must not go looking for a
    // message this branch already consumed.
    DeadlinePassed -> begin_cancel(Scope(..scope, timer: None))
    SignalLost -> begin_cancel(scope)
  }
}

/// A worker answered. Its slot stays taken until the outcome is delivered, and
/// the worker itself moves to `finishing` until its EXIT arrives.
fn note_report(scope: Scope(a, e), report: Report(a, e)) -> Scope(a, e) {
  let outcome = case report.result {
    Ok(value) -> Completed(index: report.index, value:)
    Error(error) -> Failed(index: report.index, error:)
  }
  let running = case list.key_pop(scope.running, report.worker) {
    Ok(#(_index, rest)) -> rest
    Error(Nil) -> scope.running
  }
  note_outcome(
    Scope(..scope, running:, finishing: [report.worker, ..scope.finishing]),
    outcome,
  )
}

/// A linked process died. Which one decides everything.
fn note_exit(scope: Scope(a, e), pid: Pid, reason: ExitReason) -> Scope(a, e) {
  // The caller is the only linked process that is not a worker. Its death ends
  // the run: there is nobody to deliver to, so the scope's remaining duty is to
  // make sure nothing it spawned survives it.
  use <- bool.lazy_guard(when: pid == scope.caller, return: fn() {
    detach(scope, Gone)
  })

  case list.key_pop(scope.running, pid) {
    // Still unaccounted, so this worker died instead of answering.
    Ok(#(index, rest)) ->
      note_outcome(
        Scope(..scope, running: rest),
        classify_exit(index, reason, scope.cancelling),
      )
    // Already accounted: this is the ordinary exit that follows a report, or a
    // kill that landed on a worker which had already answered.
    Error(Nil) ->
      Scope(
        ..scope,
        finishing: list.filter(scope.finishing, fn(other) { other != pid }),
      )
  }
}

/// Turn a worker's exit into an outcome.
///
/// The whole matrix is spelled out because the interesting entry is easy to get
/// wrong: the scope cancels with `process.kill`, so a cancelled worker and a
/// worker some unrelated process killed both exit with `Killed`. Only the
/// scope's own knowledge of whether it started a cancellation tells them apart,
/// and a `Killed` that arrives before any cancellation is somebody else's doing
/// and must be reported as a crash.
fn classify_exit(
  index: Int,
  reason: ExitReason,
  cancelling: Bool,
) -> Outcome(a, e) {
  case cancelling, reason {
    True, process.Killed -> Abandoned(index:)
    True, process.Normal -> Crashed(index:, reason:)
    True, process.Abnormal(..) -> Crashed(index:, reason:)
    False, process.Killed -> Crashed(index:, reason:)
    False, process.Normal -> Crashed(index:, reason:)
    False, process.Abnormal(..) -> Crashed(index:, reason:)
  }
}

/// Queue an outcome for delivery and apply the failure policy to it.
fn note_outcome(scope: Scope(a, e), outcome: Outcome(a, e)) -> Scope(a, e) {
  // A consumer that has halted or died is not owed an account, and queueing for
  // it would grow a backlog nobody will ever drain.
  let scope = case scope.consumer {
    Waiting -> Scope(..scope, ready: push_backlog(scope.ready, outcome))
    Busy -> Scope(..scope, ready: push_backlog(scope.ready, outcome))
    Halted -> scope
    Gone -> scope
  }

  let ends_the_run = case outcome {
    Failed(..) -> True
    Crashed(..) -> True
    Completed(..) -> False
    Abandoned(..) -> False
    NeverStarted(..) -> False
  }
  use <- bool.guard(when: !ends_the_run, return: scope)

  case scope.on_failure {
    KeepGoing -> scope
    CancelSiblings -> begin_cancel(scope)
  }
}

/// Stop delivering entirely: the consumer either halted or died.
fn detach(scope: Scope(a, e), consumer: Consumer) -> Scope(a, e) {
  // Clearing before cancelling, rather than after, keeps `begin_cancel` from
  // building a `NeverStarted` for every unstarted task that nobody will read.
  begin_cancel(Scope(..scope, consumer:, pending: [], ready: new_backlog()))
}

/// Cancel the run: kill every worker, then account for everything unstarted.
///
/// Kill-then-join is the ordering rule, and it lives here: every kill is sent
/// before the loop waits for a single `EXIT`. Interleaving them would let one
/// slow worker postpone the cancellation of every worker behind it, turning a
/// bounded teardown into a serial one.
fn begin_cancel(scope: Scope(a, e)) -> Scope(a, e) {
  use <- bool.guard(when: scope.cancelling, return: scope)

  list.each(scope.running, fn(slot) { process.kill(slot.0) })
  // Workers that have answered are killed too. Their outcomes already stand, so
  // this changes no account; it means cancellation leaves nothing this scope
  // spawned still running, whatever a task does after it returns.
  list.each(scope.finishing, process.kill)

  let never_started =
    list.map(scope.pending, fn(slot) { NeverStarted(index: slot.0) })

  Scope(
    ..scope,
    cancelling: True,
    pending: [],
    ready: push_all_backlog(scope.ready, never_started),
  )
}

/// Say `Done`, drop the link, and let the process end.
fn finish(scope: Scope(a, e)) -> Nil {
  flush_deadline(scope)

  case scope.consumer {
    Gone -> Nil
    Waiting -> process.send(scope.outbox, Done)
    Busy -> process.send(scope.outbox, Done)
    Halted -> process.send(scope.outbox, Done)
  }

  // Unlinking from this side means no exit signal is ever generated, which is
  // what keeps a caller that traps exits from seeing the scope's ordinary
  // finish as unexpected mailbox noise. Doing it here, after the last thing
  // that could fail, leaves the link covering the entire run.
  process.unlink(scope.caller)
}

/// Cancel the deadline timer, and drain its message if it beat us to it.
///
/// The scope's mailbox dies with the scope, so nothing can leak out of this
/// process; the flush is here so that the invariant holds where it is stated
/// rather than by accident of the process ending, and so that a stale alarm can
/// never be mistaken for a live one if this loop is ever given more to do.
fn flush_deadline(scope: Scope(a, e)) -> Nil {
  case scope.timer {
    None -> Nil
    Some(timer) ->
      case process.cancel_timer(timer) {
        process.Cancelled(_) -> Nil
        process.TimerNotFound -> {
          let _fired = process.receive(scope.alarm, 0)
          Nil
        }
      }
  }
}

// --- The delivery backlog ---------------------------------------------------
//
// Outcomes leave the scope in the order they arrived, so the scope needs a
// queue. A plain list appended at the end would be O(n) per outcome, and while
// the backlog holds at most `limit` entries in steady state, `limit` is a number
// the caller chooses: `race` sets it to the number of tasks. A pair of lists
// keeps every push O(1) and amortises the reversal across the pops.

/// A first-in-first-out queue of outcomes awaiting delivery. `front` is in
/// delivery order; `back` is in reverse arrival order and is flipped onto
/// `front` only when `front` runs dry.
type Backlog(a, e) {
  Backlog(front: List(Outcome(a, e)), back: List(Outcome(a, e)))
}

fn new_backlog() -> Backlog(a, e) {
  Backlog(front: [], back: [])
}

fn push_backlog(
  backlog: Backlog(a, e),
  outcome: Outcome(a, e),
) -> Backlog(a, e) {
  Backlog(..backlog, back: [outcome, ..backlog.back])
}

fn push_all_backlog(
  backlog: Backlog(a, e),
  outcomes: List(Outcome(a, e)),
) -> Backlog(a, e) {
  list.fold(outcomes, backlog, push_backlog)
}

fn pop_backlog(
  backlog: Backlog(a, e),
) -> Result(#(Outcome(a, e), Backlog(a, e)), Nil) {
  case backlog.front {
    [first, ..rest] -> Ok(#(first, Backlog(front: rest, back: backlog.back)))
    [] ->
      case list.reverse(backlog.back) {
        [] -> Error(Nil)
        [first, ..rest] -> Ok(#(first, Backlog(front: rest, back: [])))
      }
  }
}

fn backlog_is_empty(backlog: Backlog(a, e)) -> Bool {
  list.is_empty(backlog.front) && list.is_empty(backlog.back)
}

// --- Odds and ends ----------------------------------------------------------

/// The default fan-out: one task per scheduler.
///
/// `gleam_erlang` exposes no binding for this, and `erlang:system_info/1` is a
/// stock OTP function whose types line up with a Gleam signature directly, so
/// the external needs no shim module of its own. The single-variant private type
/// is the house idiom for naming an Erlang atom without building one at runtime.
fn default_limit() -> Int {
  int.max(1, system_info(SchedulersOnline))
}

type SystemQuery {
  SchedulersOnline
}

@external(erlang, "erlang", "system_info")
fn system_info(query: SystemQuery) -> Int

/// Restore input order from an account gathered in completion order.
fn by_index(outcomes: List(Outcome(a, e))) -> List(Outcome(a, e)) {
  list.sort(outcomes, fn(left, right) { int.compare(left.index, right.index) })
}

/// The outcomes for tasks the scope never reported on, for the one case where
/// that can happen: something destroyed the scope from outside the run.
fn unaccounted(
  gathered: List(Outcome(a, e)),
  total: Int,
  reason: ExitReason,
) -> List(Outcome(a, e)) {
  let seen = set.from_list(list.map(gathered, fn(outcome) { outcome.index }))
  int.range(from: 0, to: total, with: [], run: fn(missing, index) {
    case set.contains(seen, index) {
      True -> missing
      False -> [Crashed(index:, reason:), ..missing]
    }
  })
}
