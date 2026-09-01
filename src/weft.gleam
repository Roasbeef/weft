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
//// ## Managed tasks, and what an owner's exit proves
////
//// The contract above conflates two facts on purpose — for a leaf task,
//// "the worker exited" and "the work stopped" are the same fact. They stop
//// being the same fact the moment a task's real work lives beyond its
//// worker: an HTTP request whose socket belongs to a client library's own
//// supervisor, a database operation running under a pool. The worker can
//// return while the process holding the socket is still alive, and killing
//// the worker may kill the only process that still carries the child's
//// cancellation capability.
////
//// A *managed* task (`prepared_task`) splits the two facts apart. It
//// carries, in addition to `begin`, a published **owner** pid — the process
//// whose exit is the lifecycle truth for everything the task started — and
//// a `cancel` closure that asks that subtree to stop. The scope monitors
//// every owner *before it spawns a single worker*, so by the time `begin`
//// can touch the outside world its ownership evidence is already on file;
//// there is no window in which externally visible work exists and nobody
//// holds proof of it. A managed task's slot is then held until **both** its
//// worker and its owner have exited, and its outcome is delivered only once
//// both facts are in.
////
//// Only a *normal* owner exit proves the subtree drained. An abnormal one
//// means the proof is gone — not that the work failed, but that nobody can
//// any longer say whether it stopped — and that is a different outcome,
//// `DrainProofLost`, kept apart from `Crashed` because a caller recovering
//// resources must treat "unknown liveness" differently from "known death".
//// A `prepared_leaf` owner is the documented exemption: a task may declare
//// that its owner provably owns nothing further, and then any exit
//// completes it, so an ordinary crash of a leaf cannot manufacture a false
//// `DrainProofLost` for work that never had descendants.
////
//// The scope's own exit reason repeats the verdict outward: it exits
//// normally only if every drain was proven, and abnormally when any proof
//// was lost or left unconfirmed. That is what makes scopes compose — an
//// outer witness monitoring this scope's pid needs no protocol beyond the
//// one monitors already speak, and a scope used as another run's published
//// owner propagates a lost proof without either side writing translation
//// code.
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
//// A managed owner is never killed. Killing it would destroy the one
//// process whose exit still means something, so cancellation *asks*: the
//// task's `cancel` closure runs on a disposable helper process, once,
//// idempotently, and the scope keeps waiting for the owner's own exit. The
//// helper is disposable for the same reason it exists — a `cancel` closure
//// that crashes must cost the run a log line, not its witness. By default
//// that wait is unbounded, because "cancelled" without drain proof is not a
//// fact this module is willing to invent. `cancel_grace` bounds the wait
//// for callers that need a bounded teardown: an owner still alive when the
//// grace expires yields `CancellationUnconfirmed` — cancellation was
//// requested and nobody proved it landed — and the scope, no longer able to
//// vouch for the subtree, exits abnormally to say so.
////
//// ## Detached runs
////
//// `start` and `fold` block their caller, which is right for a function and
//// wrong for an actor that must keep serving its mailbox. `start_detached`
//// hands back a handle instead: `pull` collects one outcome at a time (the
//// same pull-based protocol, so a slow consumer still throttles the run),
//// `cancel_detached` requests cancellation without discarding the account,
//// and `scope_pid` names the scope so an outer witness can monitor it — a
//// normal exit of that pid proves the entire run, owners included, drained.
//// `start_relayed` is the push adapter for consumers that are actors: a
//// relay process owns the pulling and forwards each outcome as an ordinary
//// message, trading the engine's backpressure for the consumer's mailbox,
//// which is exactly the trade an actor's receive loop already makes.
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
import gleam/erlang/atom
import gleam/erlang/process.{type ExitReason, type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set
import weft/internal/sys

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

  /// A managed task's published owner exited abnormally, so nobody can any
  /// longer prove whether the task's transitive work stopped. This is not a
  /// worker crash — the worker's own fate is irrelevant once the proof is
  /// gone — and it is deliberately distinct from `Crashed`: a caller
  /// recovering resources must treat "unknown liveness" differently from
  /// "known death".
  DrainProofLost(
    /// The task's position in the list given to `new`, counting from zero.
    index: Int,
    /// How the owner exited. `Killed` and `Abnormal` both land here; so does
    /// an owner that was already dead when the scope went to monitor it,
    /// because a proof that was never on file was never proof.
    reason: ExitReason,
  )

  /// Cancellation was requested, the grace set by `cancel_grace` elapsed,
  /// and the task's owner was still alive: nobody proved the cancellation
  /// landed. Only a run with a grace configured can produce this — without
  /// one the scope waits for the owner's exit however long it takes.
  CancellationUnconfirmed(
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

// --- Prepared tasks ---------------------------------------------------------

/// What a published owner's exit is allowed to prove. Private: the split is
/// expressed at the API as two constructors, `prepared_task` and
/// `prepared_leaf`, so a call site names the claim it is making rather than
/// passing a flag.
type OwnerRole {
  /// The owner may have descendants of its own, so only its *normal* exit
  /// proves the subtree drained; anything else is a lost proof.
  Transitive

  /// The owner provably owns nothing further. Any exit completes it, which
  /// is what keeps an ordinary crash of a leaf from manufacturing a false
  /// `DrainProofLost` for work that never had descendants.
  Leaf
}

/// One task, prepared for a run: at minimum a `begin` closure, and for
/// managed work also the published owner whose exit is the lifecycle truth
/// for everything the task starts.
///
/// Built with `task`, `prepared_task` or `prepared_leaf`, and run with
/// `new_prepared`. The owner must already be alive when the run starts —
/// that is the *parked work* pattern: prepare the resource-holding process
/// first, hand its pid here, and let `begin` release the actual work only
/// once it runs inside the scope, which is by construction after the scope
/// has the owner under monitor.
pub opaque type PreparedTask(a, e) {
  /// A plain task: worker exit and work stopping are the same fact.
  PlainTask(begin: fn() -> Result(a, e))

  /// A managed task: the work persists beyond the worker, and `owner`'s
  /// exit is what settles it.
  OwnedTask(
    owner: Pid,
    cancel: fn() -> Nil,
    role: OwnerRole,
    begin: fn() -> Result(a, e),
  )
}

/// Prepare a plain task: no owner, no drain obligation, exactly the
/// behaviour of a closure given to `new`.
///
/// ## Examples
///
/// ```gleam
/// let outcomes =
///   weft.new_prepared([weft.task(fn() { Ok(1) })])
///   |> weft.start
/// ```
pub fn task(begin: fn() -> Result(a, e)) -> PreparedTask(a, e) {
  PlainTask(begin:)
}

/// Prepare a managed task: work whose lifecycle outlives its worker.
///
/// `owner` is the pid whose exit proves the task's transitive work is gone —
/// only a *normal* exit proves it; any other exit becomes `DrainProofLost`.
/// `cancel` asks the subtree to stop; it must be safe to call more than once
/// and safe to call after the work has already finished, and it runs on a
/// disposable helper so a crash inside it cannot cost the run its witness.
/// `begin` is the worker's blocking path, and it runs only after the scope
/// holds the owner under monitor.
///
/// The task's slot is held, and its outcome withheld, until both the worker
/// and the owner have exited. An owner already dead when the run starts
/// yields `DrainProofLost` without `begin` ever running: proof that was
/// never on file was never proof.
///
/// ## Examples
///
/// ```gleam
/// // `prepare` parks the real request and hands back its owning pid, a
/// // cancel capability, and the closure that lets it loose.
/// let #(owner, cancel, begin) = prepare(request)
///
/// let outcomes =
///   weft.new_prepared([weft.prepared_task(owner:, cancel:, begin:)])
///   |> weft.cancel_grace(2000)
///   |> weft.start
/// ```
pub fn prepared_task(
  owner owner: Pid,
  cancel cancel: fn() -> Nil,
  begin begin: fn() -> Result(a, e),
) -> PreparedTask(a, e) {
  OwnedTask(owner:, cancel:, role: Transitive, begin:)
}

/// Prepare a managed task whose owner provably owns nothing further.
///
/// The one difference from `prepared_task`: *any* exit of the owner
/// completes the drain obligation, normal or not. This is the declared
/// exemption for single-process owners — an observer, a pump, a relay —
/// where an ordinary crash is an ordinary crash and must not be read as a
/// lost proof over descendants that never existed. Everything else — the
/// monitor-before-begin ordering, the held slot, the `cancel` helper — is
/// identical.
///
/// ## Examples
///
/// ```gleam
/// let outcomes =
///   weft.new_prepared([
///     weft.prepared_leaf(owner: pump, cancel: stop_pump, begin: run),
///   ])
///   |> weft.start
/// ```
pub fn prepared_leaf(
  owner owner: Pid,
  cancel cancel: fn() -> Nil,
  begin begin: fn() -> Result(a, e),
) -> PreparedTask(a, e) {
  OwnedTask(owner:, cancel:, role: Leaf, begin:)
}

// --- The builder ------------------------------------------------------------

/// A configured run, not yet started.
///
/// Built with `new` or `new_prepared` and refined with `limit`, `on_failure`,
/// `cancel_with`, `deadline` and `cancel_grace`; `start`, `fold` and
/// `start_detached` are the terminal verbs. A `Run` is an ordinary immutable
/// value, so the same one can be started more than once.
pub opaque type Run(a, e) {
  Run(
    tasks: List(PreparedTask(a, e)),
    limit: Int,
    on_failure: OnFailure,
    signal: Option(Cancel),
    within: Option(Int),
    grace: Option(Int),
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
  new_prepared(list.map(tasks, task))
}

/// Begin a run over prepared tasks, plain and managed mixed freely.
///
/// This is `new` for tasks built with `task`, `prepared_task` and
/// `prepared_leaf`. The defaults are the same: a limit of the schedulers
/// online, `KeepGoing` on failure, no deadline, and no cancellation grace —
/// a cancelled managed task is awaited until its owner exits, however long
/// that takes, unless `cancel_grace` bounds it.
///
/// ## Examples
///
/// ```gleam
/// let outcomes =
///   weft.new_prepared([
///     weft.task(fn() { fetch(url) }),
///     weft.prepared_task(owner:, cancel:, begin:),
///   ])
///   |> weft.start
/// ```
pub fn new_prepared(tasks: List(PreparedTask(a, e))) -> Run(a, e) {
  Run(
    tasks:,
    limit: default_limit(),
    on_failure: KeepGoing,
    signal: None,
    within: None,
    grace: None,
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

/// Bound how long a cancellation waits for managed owners to exit, in
/// milliseconds.
///
/// Without a grace, cancelling a managed task means asking its owner to stop
/// and waiting for the owner's exit however long that takes, because
/// "cancelled" without drain proof is not a fact this module will invent.
/// With one, an owner still alive when the grace expires settles as
/// `CancellationUnconfirmed`, the run finishes bounded — and the scope exits
/// abnormally, because it can no longer vouch for the subtree it was
/// witnessing. The grace bounds the *wait*, and what it buys in boundedness
/// it pays for in the scope's exit verdict.
///
/// One grace timer serves the whole cancellation rather than one per task:
/// it is an acknowledgement window on the teardown, not a per-task timeout,
/// and it is armed when cancellation begins, from whichever direction it
/// came.
///
/// ## Examples
///
/// ```gleam
/// let outcomes =
///   weft.new_prepared(tasks)
///   |> weft.deadline(30_000)
///   |> weft.cancel_grace(2000)
///   |> weft.start
/// ```
pub fn cancel_grace(run: Run(a, e), within: Int) -> Run(a, e) {
  Run(..run, grace: Some(int.max(0, within)))
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
///       weft.DrainProofLost(..) -> weft.Continue(count)
///       weft.CancellationUnconfirmed(..) -> weft.Continue(count)
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
///       weft.DrainProofLost(..) | weft.CancellationUnconfirmed(..) -> found
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

// --- Detached runs ----------------------------------------------------------

/// A handle to a run whose caller declined to block.
///
/// Handed back by `start_detached`. The holder collects outcomes with
/// `pull`, requests cancellation with `cancel_detached`, and — the part a
/// blocking caller never needs — can hand `scope_pid` to a monitor: the
/// scope's *normal* exit proves the entire run, managed owners included,
/// drained, and an abnormal exit says some proof was lost or unconfirmed.
///
/// The handle's outbox belongs to the process that called `start_detached`,
/// so `pull` must be called from that same process. The scope is linked to
/// that process too: if the holder dies, the scope survives the exit signal
/// (it traps), cancels what is running, asks every managed owner to stop,
/// and drains before exiting — a detached run abandoned by its holder tears
/// itself down rather than leaking.
pub opaque type Detached(a, e) {
  Detached(
    /// The scope process, for outer witnesses to monitor.
    scope: Pid,
    /// Where demand and cancellation are sent.
    inbox: Subject(Request),
    /// Where the scope's replies land. Owned by the starting process.
    outbox: Subject(Reply(a, e)),
  )
}

/// What one `pull` produced.
pub type Pulled(a, e) {
  /// One task's outcome, in completion order.
  PulledOutcome(
    /// The outcome pulled.
    outcome: Outcome(a, e),
  )

  /// Every outcome has been delivered; the run is over and nothing it
  /// started is alive. Stop pulling: there is nothing left to pull, and a
  /// later `pull` will report the scope's exit as `RunLost` rather than
  /// repeating this answer.
  AllDelivered

  /// Nothing landed within the wait. The demand stands — the scope holds at
  /// most one granted delivery, so pulling again neither loses nor
  /// duplicates an outcome.
  NotYet

  /// The scope died without saying `Done`: either something outside the run
  /// destroyed it, or its final exit carried the drain verdict for a run
  /// whose account was already delivered.
  RunLost(
    /// How the scope exited.
    reason: ExitReason,
  )
}

/// Start a run without blocking, and hand back its handle.
///
/// The run is the same in every respect — same scope, same account, same
/// ownership guarantees — only the consumption differs: nothing is
/// delivered until the holder asks with `pull`, so the run's backpressure
/// now reaches whatever pace the holder pulls at.
///
/// ## Examples
///
/// ```gleam
/// let detached =
///   weft.new_prepared(tasks)
///   |> weft.cancel_grace(2000)
///   |> weft.start_detached
///
/// let watch = process.monitor(weft.scope_pid(detached))
/// // ... pull outcomes as the mood takes you ...
/// ```
pub fn start_detached(run: Run(a, e)) -> Detached(a, e) {
  let outbox = process.new_subject()
  let caller = process.self()
  let scope = process.spawn(fn() { run_scope(caller, outbox, run, Busy) })

  // The scope's first word is `Ready`, sent before its loop begins, so this
  // receive is a handshake rather than a wait on work. The monitor covers
  // the one way the handshake can fail — the scope dying first — and a dead
  // scope still yields a usable handle: its inbox goes nowhere, and every
  // `pull` reports `RunLost`.
  let watch = process.monitor(scope)
  let hello =
    process.new_selector()
    |> process.select_map(outbox, FromScope)
    |> process.select_specific_monitor(watch, scope_down)
  let inbox = await_ready(hello)
  process.demonitor_process(watch)

  Detached(scope:, inbox:, outbox:)
}

/// Wait for the scope's `Ready`, or fabricate an inbox if it died first.
fn await_ready(hello: process.Selector(CallerEvent(a, e))) -> Subject(Request) {
  case process.selector_receive_forever(hello) {
    FromScope(Ready(inbox:)) -> inbox
    ScopeDown(..) -> process.new_subject()

    // `Ready` precedes every delivery by construction; these arms are
    // totality, not races we have seen.
    FromScope(Delivered(inbox:, ..)) -> inbox
    FromScope(Done) -> process.new_subject()
  }
}

/// Collect one outcome from a detached run, waiting at most `within`
/// milliseconds.
///
/// Must be called from the process that called `start_detached` — the
/// handle's outbox belongs to it. Demand is unary and idempotent: a `pull`
/// that times out leaves its demand standing, and the next `pull` re-grants
/// it harmlessly, so at most one outcome is ever in flight and none is ever
/// dropped or doubled.
///
/// ## Examples
///
/// ```gleam
/// case weft.pull(detached, within: 1000) {
///   weft.PulledOutcome(outcome) -> act_on(outcome)
///   weft.NotYet -> check_something_else()
///   weft.AllDelivered -> done()
///   weft.RunLost(reason) -> escalate(reason)
/// }
/// ```
pub fn pull(detached: Detached(a, e), within timeout: Int) -> Pulled(a, e) {
  process.send(detached.inbox, Next)

  // The monitor is per-pull rather than per-handle so that `pull` stays
  // callable after any answer: demonitoring flushes, so no stale `DOWN`
  // survives into the holder's later receives.
  let watch = process.monitor(detached.scope)
  let events =
    process.new_selector()
    |> process.select_map(detached.outbox, FromScope)
    |> process.select_specific_monitor(watch, scope_down)

  let pulled = await_pull(events, timeout)
  process.demonitor_process(watch)
  pulled
}

/// One pull's receive. `Ready` is the handshake echo a first pull can still
/// find in the mailbox; it is consumed and the wait continues.
fn await_pull(
  events: process.Selector(CallerEvent(a, e)),
  timeout: Int,
) -> Pulled(a, e) {
  case process.selector_receive(events, timeout) {
    Ok(FromScope(Delivered(outcome:, ..))) -> PulledOutcome(outcome:)
    Ok(FromScope(Done)) -> AllDelivered
    Ok(FromScope(Ready(..))) -> await_pull(events, timeout)
    Ok(ScopeDown(reason: process.Normal)) -> AllDelivered
    Ok(ScopeDown(reason:)) -> RunLost(reason:)
    Error(Nil) -> NotYet
  }
}

/// Cancel a detached run without giving up its account.
///
/// Unlike a `fold` reducer's `Halt`, which discards what it has not seen,
/// this keeps the deliveries coming: running tasks settle as `Abandoned`,
/// unstarted ones as `NeverStarted`, and managed tasks by whatever their
/// owners' exits prove. Idempotent — cancelling a cancelled or finished run
/// does nothing.
///
/// ## Examples
///
/// ```gleam
/// weft.cancel_detached(detached)
/// // ... keep pulling until AllDelivered ...
/// ```
pub fn cancel_detached(detached: Detached(a, e)) -> Nil {
  process.send(detached.inbox, CancelRun)
}

/// The scope process behind a detached run.
///
/// This is the pid an outer witness monitors, and the pid a *nested* run
/// publishes as its owner: a detached run handed to `prepared_task` as
/// `owner: weft.scope_pid(inner)` composes ownership — the outer scope's
/// proof for that task is the inner scope's own drain verdict, with no
/// translation code on either side.
///
/// ## Examples
///
/// ```gleam
/// let inner = weft.new_prepared(children) |> weft.start_detached
///
/// let outer_task =
///   weft.prepared_task(
///     owner: weft.scope_pid(inner),
///     cancel: fn() { weft.cancel_detached(inner) },
///     begin: fn() { collect(inner) },
///   )
/// ```
pub fn scope_pid(detached: Detached(a, e)) -> Pid {
  detached.scope
}

/// Start a run and push every outcome to `sink` as an ordinary message,
/// from a relay process this function spawns and returns.
///
/// This is the adapter for consumers that are actors: an actor must not
/// block its receive loop inside `pull`, so the relay owns the pulling and
/// the actor merely receives. Each outcome arrives as `PulledOutcome`,
/// followed by exactly one `AllDelivered` or `RunLost`, after which the
/// relay exits normally.
///
/// What is traded away is stated plainly: push delivery makes the sink's
/// mailbox the buffer, so the engine's backpressure ends at the relay. That
/// is the same trade every actor's mailbox already makes, and the `limit`
/// still bounds how much work runs at once — only the finished outcomes
/// queue without bound.
///
/// The relay is linked to the caller, and the scope to the relay, so the
/// ownership chain survives: a dead consumer takes the relay with it, the
/// scope traps that exit, cancels, drains its owners, and exits.
///
/// ## Examples
///
/// ```gleam
/// let relay =
///   weft.new_prepared(tasks)
///   |> weft.start_relayed(to: sink)
/// // The actor's selector now receives weft.Pulled(a, e) messages.
/// ```
pub fn start_relayed(run: Run(a, e), to sink: Subject(Pulled(a, e))) -> Pid {
  process.spawn(fn() {
    let detached = start_detached(run)

    // The relay grants demand once, then re-grants it after every delivery:
    // the pull protocol unchanged, with the relay standing where a blocked
    // caller would.
    let watch = process.monitor(detached.scope)
    let events =
      process.new_selector()
      |> process.select_map(detached.outbox, FromScope)
      |> process.select_specific_monitor(watch, scope_down)
    relay_outcomes(detached, events, sink)
  })
}

/// The relay's loop: forward until the run says it is over, then stop.
fn relay_outcomes(
  detached: Detached(a, e),
  events: process.Selector(CallerEvent(a, e)),
  sink: Subject(Pulled(a, e)),
) -> Nil {
  process.send(detached.inbox, Next)
  case process.selector_receive_forever(events) {
    FromScope(Delivered(outcome:, ..)) -> {
      process.send(sink, PulledOutcome(outcome:))
      relay_outcomes(detached, events, sink)
    }
    FromScope(Ready(..)) -> relay_outcomes(detached, events, sink)
    FromScope(Done) -> process.send(sink, AllDelivered)
    ScopeDown(reason: process.Normal) -> process.send(sink, AllDelivered)
    ScopeDown(reason:) -> process.send(sink, RunLost(reason:))
  }
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
        DrainProofLost(..) -> Continue(account)
        CancellationUnconfirmed(..) -> Continue(account)
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
      DrainProofLost(..) -> Error(Nil)
      CancellationUnconfirmed(..) -> Error(Nil)
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
        DrainProofLost(..) -> #(succeeded, [outcome, ..rest])
        CancellationUnconfirmed(..) -> #(succeeded, [outcome, ..rest])
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
      DrainProofLost(..) -> Error(Nil)
      CancellationUnconfirmed(..) -> Error(Nil)
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
      DrainProofLost(..) -> Error(Nil)
      CancellationUnconfirmed(..) -> Error(Nil)
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
  /// The scope's first word, sent before anything else: here is the inbox.
  /// A blocking caller ignores it — `Delivered` re-carries the inbox anyway —
  /// but a detached caller needs the inbox *before* the first delivery, to
  /// have somewhere to send demand and cancellation.
  Ready(inbox: Subject(Request))

  Delivered(inbox: Subject(Request), outcome: Outcome(a, e))

  Done
}

/// What the caller says back. Nothing else is ever sent.
type Request {
  /// The outcome has been consumed; send the next one when there is one.
  /// Demand is unary, not counted: a `Next` while the scope is already
  /// allowed to deliver is a no-op, which is what lets a detached puller
  /// re-grant demand it cannot remember granting without ever doubling a
  /// delivery.
  Next

  /// The reducer halted. Cancel the rest and tell me when the teardown is done.
  Stop

  /// Cancel the run, but keep delivering the account. This is the detached
  /// handle's cancellation: unlike `Stop`, the caller still wants to hear
  /// what happened to every task.
  CancelRun
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
  let outbox = process.new_subject()
  let caller = process.self()

  // proc_lib:spawn_link, so the link is in place before the scope runs its
  // first instruction. A caller that dies during the handshake is therefore
  // still a caller the scope will hear about. A blocking caller counts as
  // one unit of demand already granted, because it is about to sit in
  // `consume` with nothing else to do.
  let scope = process.spawn(fn() { run_scope(caller, outbox, run, Waiting) })

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

    // The hello matters only to a detached caller; a blocking one gets the
    // inbox with every delivery.
    FromScope(Ready(..)) -> consume(replies, accumulator, reducer)

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
    FromScope(Ready(..)) -> await_done(replies)
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

  /// The cancellation grace ran out with owners still unaccounted for.
  GracePassed

  /// A monitored process died: the cancel signal or a managed owner. Which
  /// one is the dispatch's problem — the selector only knows it was watched.
  WatchedDown(pid: Pid, reason: ExitReason)

  /// A monitored *port* died. The scope never monitors ports, so this is
  /// totality for the generic monitor arm, not an event with a meaning.
  PortWatched

  /// Something arrived on the OTP system plane: `sys:get_state/1`,
  /// `sys:suspend/1` and their kin. Answering these is what makes a scope
  /// visible to the observer and freezable like any other OTP process.
  System(incoming: sys.Incoming)
}

/// Which fact a managed owner's exit has established so far.
type Proof {
  /// The owner is still alive; the task's outcome is withheld.
  ProofPending

  /// The owner was already dead when the scope went to adopt it. Its
  /// `noproc` `DOWN` is queued and will settle the task as lost whatever
  /// role it was declared with: a leaf that was gone before `begin` could
  /// run proved nothing about work that never started, and its task must
  /// still appear in the account.
  ProofAbsent

  /// The owner's exit proved the subtree drained. The task's own outcome
  /// stands.
  ProofDrained

  /// The owner exited without proving anything: the task settles as
  /// `DrainProofLost`, whatever its worker did.
  ProofLost(reason: ExitReason)

  /// The cancellation grace expired with the owner alive: the task settles
  /// as `CancellationUnconfirmed`.
  ProofUnconfirmed
}

/// One managed owner under watch, from the moment the scope starts to the
/// moment its proof resolves.
type OwnerSlot {
  OwnerSlot(
    /// The task this owner vouches for.
    index: Int,
    /// The owner process itself. Never killed by this scope: its exit is
    /// the one fact cancellation is waiting to learn.
    pid: Pid,
    /// The monitor watching it, kept so a grace expiry can demonitor with a
    /// flush rather than leave a stale `DOWN` for the loop to misread.
    monitor: process.Monitor,
    /// What this owner's exit is allowed to prove.
    role: OwnerRole,
    /// The idempotent ask-to-stop capability, run on a disposable helper.
    cancel: fn() -> Nil,
    /// Where the proof stands.
    proof: Proof,
    /// The helper running `cancel`, once cancellation has dispatched it.
    canceller: Option(Pid),
  )
}

/// The run-wide drain verdict, carried out of the scope as its exit reason.
/// Strictly ordered: a lost proof outranks an unconfirmed cancellation,
/// which outranks a clean drain, and `worsen` only ever moves up.
type Verdict {
  AllDrained

  SomeUnconfirmed

  SomeLost
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
    grace_alarm: Subject(Nil),
    events: process.Selector(Event(a, e)),
    /// The suspended-mode selector: the system plane and nothing else, so a
    /// frozen scope stays frozen while `sys` can still reach it.
    sys_events: process.Selector(Event(a, e)),
    on_failure: OnFailure,
    timer: Option(process.Timer),
    grace: Option(Int),
    grace_timer: Option(process.Timer),
    /// Tasks that have not been spawned, in input order.
    pending: List(#(Int, fn() -> Result(a, e))),
    /// Spawned and unaccounted, keyed by pid because that is what an `EXIT`
    /// gives us.
    running: List(#(Pid, Int)),
    /// Accounted but not yet reaped. Their only remaining act is to exit.
    finishing: List(Pid),
    /// Outcomes waiting for the consumer to ask, in completion order.
    ready: Backlog(a, e),
    /// Worker-settled outcomes withheld because their owner's proof is
    /// still pending, keyed by task index.
    awaiting: List(#(Int, Outcome(a, e))),
    /// Every managed owner, resolved and not. Consulted at settle time for
    /// each outcome, and by `settled` for whether the run may end.
    owners: List(OwnerSlot),
    /// Disposable helpers running `cancel` closures. Their exits are
    /// bookkeeping, not outcomes, but the scope will not return while one
    /// is alive.
    helpers: List(Pid),
    /// The cancel signal's pid, so its `DOWN` can be told apart from an
    /// owner's.
    signal_pid: Option(Pid),
    /// Free slots. Taken at spawn, returned at delivery. Not consulted once
    /// `cancelling` is `True`, since nothing new is spawned after that.
    slots: Int,
    consumer: Consumer,
    cancelling: Bool,
    /// The drain verdict so far. Only ever worsens, and leaves this process
    /// as its exit reason.
    verdict: Verdict,
    /// The OTP debug plane: suspend/resume mode and what `sys` reports.
    plane: sys.Plane,
  )
}

/// The scope process, from `trap_exits` to its last `EXIT`.
fn run_scope(
  caller: Pid,
  outbox: Subject(Reply(a, e)),
  run: Run(a, e),
  consumer: Consumer,
) -> Nil {
  // Trapping first is what turns a worker crash into a `Crashed` outcome
  // instead of a dead scope, and what turns the caller's death into an event
  // this process can act on rather than a signal that kills it mid-teardown.
  process.trap_exits(True)

  let Run(tasks:, limit:, on_failure:, signal:, within:, grace:) = run
  let indexed =
    list.index_map(tasks, fn(prepared, index) { #(index, prepared) })

  let inbox = process.new_subject()
  let reports = process.new_subject()
  let alarm = process.new_subject()
  let grace_alarm = process.new_subject()

  // The hello precedes everything, deliveries included: a detached caller
  // has no other way to learn where demand and cancellation are sent.
  process.send(outbox, Ready(inbox:))

  let timer = case within {
    None -> None
    Some(milliseconds) -> Some(process.send_after(alarm, milliseconds, Nil))
  }

  // The system arm is merged last so nothing the run selects can shadow it;
  // a scope that stopped answering `sys` would hang the very tool reaching
  // for it.
  let events =
    process.new_selector()
    |> process.select_map(reports, Reported)
    |> process.select_map(inbox, Asked)
    |> process.select_map(alarm, fn(_) { DeadlinePassed })
    |> process.select_map(grace_alarm, fn(_) { GracePassed })
    |> process.select_trapped_exits(fn(exit: process.ExitMessage) {
      Exited(pid: exit.pid, reason: exit.reason)
    })
    |> process.select_monitors(watched_down)
    |> sys.selecting(System)

  let sys_events =
    process.new_selector()
    |> sys.selecting(System)

  // Owners before workers, unconditionally: adopting every owner here, ahead
  // of the first `fill_slots`, is what makes "the scope holds ownership
  // evidence before `begin` runs" true by construction rather than by
  // handshake. An owner that is already dead queues its `DOWN` immediately,
  // so the ordinary dispatch catches it before its task's worker can spawn.
  let #(owners, pending) = adopt_owners(indexed)

  let scope =
    Scope(
      caller:,
      outbox:,
      inbox:,
      reports:,
      alarm:,
      grace_alarm:,
      events:,
      sys_events:,
      on_failure:,
      timer:,
      grace:,
      grace_timer: None,
      pending:,
      running: [],
      finishing: [],
      ready: new_backlog(),
      awaiting: [],
      owners:,
      helpers: [],
      signal_pid: None,
      slots: limit,
      consumer:,
      cancelling: False,
      verdict: AllDrained,
      plane: sys.new(module: "weft", parent: caller),
    )

  loop(watch_signal(scope, signal))
}

/// Split prepared tasks into the owner ledger and the spawn queue, taking
/// out a monitor on every owner as it passes.
fn adopt_owners(
  tasks: List(#(Int, PreparedTask(a, e))),
) -> #(List(OwnerSlot), List(#(Int, fn() -> Result(a, e)))) {
  let #(owners, pending) =
    list.fold(tasks, #([], []), fn(split, entry) {
      let #(owners, pending) = split
      let #(index, prepared) = entry

      case prepared {
        PlainTask(begin:) -> #(owners, [#(index, begin), ..pending])

        OwnedTask(owner:, cancel:, role:, begin:) -> {
          let monitor = process.monitor(owner)

          // The `DOWN` for an already-dead owner is queued by the monitor
          // call, but `fill_slots` runs before the scope's first receive, so
          // the message alone arrives too late to stop the spawn. This
          // liveness check is what actually holds `begin` back; the queued
          // `DOWN` then resolves the proof — carrying the real reason —
          // through the ordinary dispatch. The monitor still covers an
          // owner that dies between these two lines. Absence is recorded on
          // the slot as well, because a task whose worker never spawns has
          // no exit of its own to settle it: the `DOWN` must settle it as
          // lost even for a leaf, or the account would come up one short.
          let #(proof, pending) = case process.is_alive(owner) {
            True -> #(ProofPending, [#(index, begin), ..pending])
            False -> #(ProofAbsent, pending)
          }
          let slot =
            OwnerSlot(
              index:,
              pid: owner,
              monitor:,
              role:,
              cancel:,
              proof:,
              canceller: None,
            )
          #([slot, ..owners], pending)
        }
      }
    })

  #(owners, list.reverse(pending))
}

/// Sort a generic monitor `DOWN` into the event vocabulary. Ports are
/// unreachable — the scope never monitors one — but the type owns them.
fn watched_down(down: process.Down) -> Event(a, e) {
  case down {
    process.ProcessDown(pid:, reason:, ..) -> WatchedDown(pid:, reason:)
    process.PortDown(..) -> PortWatched
  }
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

      let scope = Scope(..scope, signal_pid: Some(pid))
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
  // A suspended scope serves the system plane and nothing else: run events
  // queue in the mailbox until `sys:resume/1`, which is the promise
  // `sys:suspend/1` made on this process's behalf.
  use <- bool.lazy_guard(when: sys.is_suspended(scope.plane), return: fn() {
    loop(step(scope, process.selector_receive_forever(scope.sys_events)))
  })

  let scope = deliver(fill_slots(scope))
  case settled(scope) {
    True -> finish(scope)
    False -> loop(step(scope, process.selector_receive_forever(scope.events)))
  }
}

/// Is there any work, any unreaped worker, any undelivered outcome, any
/// unresolved owner, or any live helper left?
///
/// The `finishing` clause is the one that is easy to drop and expensive to lose:
/// without it the scope could return while a worker that had already answered
/// was still alive, which is exactly the guarantee this module sells. The
/// owner and helper clauses extend the same guarantee to managed work: a
/// scope that returned while an owner's proof was pending would be a witness
/// that walked out mid-testimony.
fn settled(scope: Scope(a, e)) -> Bool {
  list.is_empty(scope.pending)
  && list.is_empty(scope.running)
  && list.is_empty(scope.finishing)
  && backlog_is_empty(scope.ready)
  && list.is_empty(scope.awaiting)
  && list.is_empty(scope.helpers)
  && list.all(scope.owners, owner_resolved)
}

/// Has this owner's exit — or the grace — said what it is going to say?
fn owner_resolved(slot: OwnerSlot) -> Bool {
  case slot.proof {
    ProofPending -> False
    ProofAbsent -> False
    ProofDrained -> True
    ProofLost(..) -> True
    ProofUnconfirmed -> True
  }
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
    Asked(request: Next) ->
      Scope(..scope, consumer: note_demand(scope.consumer))
    Asked(request: Stop) -> detach(scope, Halted)
    Asked(request: CancelRun) -> begin_cancel(scope)

    // Clearing the timer here is half of the flush: a timer that has fired
    // cannot be cancelled, and `flush_deadline` must not go looking for a
    // message this branch already consumed.
    DeadlinePassed -> begin_cancel(Scope(..scope, timer: None))
    GracePassed -> expire_grace(Scope(..scope, grace_timer: None))
    WatchedDown(pid:, reason:) -> note_watched_down(scope, pid, reason)
    PortWatched -> scope

    // The reply to a system request is sent from inside `handle`, before the
    // loop can touch another message; only the mode comes back.
    System(incoming: sys.Request(message:)) ->
      Scope(..scope, plane: sys.handle(scope.plane, message, holding: scope))
    System(incoming: sys.Unimplemented(..)) -> {
      sys.warn("weft scope ignoring an unimplemented system request")
      scope
    }
  }
}

/// Grant one unit of demand, if there is still anyone to deliver to.
///
/// Demand is unary rather than counted, which is what makes a detached
/// puller's re-sent `Next` a no-op instead of a doubled delivery. A halted
/// or dead consumer stays halted or dead: a late `Next` from a mailbox race
/// must not reopen delivery.
fn note_demand(consumer: Consumer) -> Consumer {
  case consumer {
    Waiting -> Waiting
    Busy -> Waiting
    Halted -> Halted
    Gone -> Gone
  }
}

/// A watched process died: the cancel signal, or a managed owner.
fn note_watched_down(
  scope: Scope(a, e),
  pid: Pid,
  reason: ExitReason,
) -> Scope(a, e) {
  use <- bool.lazy_guard(when: Some(pid) == scope.signal_pid, return: fn() {
    begin_cancel(scope)
  })
  resolve_owner(scope, pid, reason)
}

/// An owner has exited; record what that proves and act on it.
fn resolve_owner(
  scope: Scope(a, e),
  pid: Pid,
  reason: ExitReason,
) -> Scope(a, e) {
  case find_owner(scope.owners, pid) {
    // A `DOWN` with no slot is a flushed monitor's leftover or somebody
    // else's monitor traffic; there is nothing of ours in it.
    Error(Nil) -> scope

    Ok(slot) -> {
      // The canceller's job ended with the owner, however the owner went.
      // Dismissed off the slot as found, before the slot is rewritten
      // without it — a helper whose `cancel` blocks would otherwise hold
      // the scope open after the very exit it was asking for.
      dismiss_canceller(slot.canceller)

      let proof = judge_exit(slot, reason)
      let slot = OwnerSlot(..slot, proof:, canceller: None)

      let scope =
        Scope(
          ..scope,
          owners: put_owner(scope.owners, slot),
          verdict: worsen_for(scope.verdict, proof),
        )
      apply_proof(scope, slot)
    }
  }
}

/// What one exit reason proves, given what the owner was declared to be
/// and whether it was ever alive under watch.
///
/// The full matrix is spelled out because the corner that matters hides in
/// it: a `noproc` `DOWN` — the owner was dead before the scope could watch
/// it — arrives as an abnormal reason, and that is a lost proof whatever
/// the role, because a leaf exemption covers an ordinary crash of work
/// that ran, not an owner that was a corpse before `begin` was admitted.
/// Proof that was never on file was never proof. For an owner that was
/// alive at adoption, the role decides: any exit completes a leaf, and
/// only a normal exit proves a transitive subtree drained.
fn judge_exit(slot: OwnerSlot, reason: ExitReason) -> Proof {
  case slot.proof {
    ProofAbsent -> ProofLost(reason:)
    ProofPending -> judge_role(slot.role, reason)

    // A monitor fires once, so a resolved slot never sees a second exit;
    // the role still answers, for totality.
    ProofDrained -> judge_role(slot.role, reason)
    ProofLost(..) -> judge_role(slot.role, reason)
    ProofUnconfirmed -> judge_role(slot.role, reason)
  }
}

/// The role's half of the judgement, for an owner that was alive under
/// watch.
fn judge_role(role: OwnerRole, reason: ExitReason) -> Proof {
  case role, reason {
    Leaf, process.Normal -> ProofDrained
    Leaf, process.Killed -> ProofDrained
    Leaf, process.Abnormal(..) -> ProofDrained
    Transitive, process.Normal -> ProofDrained
    Transitive, process.Killed -> ProofLost(reason:)
    Transitive, process.Abnormal(..) -> ProofLost(reason:)
  }
}

/// Fold one owner's proof into the run-wide verdict.
fn worsen_for(verdict: Verdict, proof: Proof) -> Verdict {
  case proof {
    ProofLost(..) -> SomeLost
    ProofUnconfirmed -> worsen_to_unconfirmed(verdict)
    ProofPending -> verdict
    ProofAbsent -> verdict
    ProofDrained -> verdict
  }
}

/// Unconfirmed never downgrades a lost proof.
fn worsen_to_unconfirmed(verdict: Verdict) -> Verdict {
  case verdict {
    SomeLost -> SomeLost
    SomeUnconfirmed -> SomeUnconfirmed
    AllDrained -> SomeUnconfirmed
  }
}

/// Act on a freshly resolved proof: release a withheld outcome, or reach
/// forward into work that has not settled yet.
fn apply_proof(scope: Scope(a, e), slot: OwnerSlot) -> Scope(a, e) {
  case list.key_pop(scope.awaiting, slot.index) {
    // The worker already settled and its outcome was withheld on this very
    // proof; seal and queue it.
    Ok(#(outcome, awaiting)) ->
      queue_outcome(
        Scope(..scope, awaiting:),
        seal_outcome(outcome, slot.proof),
      )

    Error(Nil) -> reach_forward(scope, slot)
  }
}

/// A proof resolved before the worker settled. A drained proof asks for
/// nothing — the worker's own settle will read it. A lost one reaches
/// forward: an unstarted `begin` must never run under a dead witness, and a
/// running worker's continued work is unsupervisable, so it is killed and
/// its exit settles through the ordinary path.
fn reach_forward(scope: Scope(a, e), slot: OwnerSlot) -> Scope(a, e) {
  case slot.proof {
    ProofLost(reason:) ->
      case list.key_pop(scope.pending, slot.index) {
        Ok(#(_begin, pending)) ->
          queue_outcome(
            Scope(..scope, pending:),
            DrainProofLost(index: slot.index, reason:),
          )
        Error(Nil) -> settle_lost_worker(scope, slot.index, reason)
      }

    ProofPending -> scope
    ProofAbsent -> scope
    ProofDrained -> scope
    ProofUnconfirmed -> scope
  }
}

/// A proof was lost for a task that is neither pending nor withheld. A
/// running worker is killed — its work is unsupervisable under a dead
/// witness — and its `EXIT` settles through `note_exit`, where the seal
/// turns whatever that classifies into `DrainProofLost`. A task with no
/// worker at all — its owner was already dead at adoption, so `begin` was
/// never allowed to spawn — settles here directly, because no exit is ever
/// going to arrive on its behalf.
fn settle_lost_worker(
  scope: Scope(a, e),
  index: Int,
  reason: ExitReason,
) -> Scope(a, e) {
  let killed =
    list.find(scope.running, fn(slot) {
      let #(_pid, running_index) = slot
      running_index == index
    })
  case killed {
    Ok(#(pid, _index)) -> {
      process.kill(pid)
      scope
    }
    Error(Nil) -> queue_outcome(scope, DrainProofLost(index:, reason:))
  }
}

/// End a canceller whose question has been answered.
fn dismiss_canceller(canceller: Option(Pid)) -> Nil {
  case canceller {
    None -> Nil
    Some(pid) -> process.kill(pid)
  }
}

/// The grace ran out. Every owner still pending settles as unconfirmed: its
/// monitor is flushed away, its canceller dismissed, and any withheld
/// outcome released as `CancellationUnconfirmed`. An owner whose `DOWN` was
/// already in the mailbox was resolved before this event — mailbox order —
/// so only genuinely silent owners land here.
fn expire_grace(scope: Scope(a, e)) -> Scope(a, e) {
  list.fold(scope.owners, scope, fn(scope, slot) {
    case slot.proof {
      ProofPending -> {
        process.demonitor_process(slot.monitor)
        dismiss_canceller(slot.canceller)

        let slot = OwnerSlot(..slot, proof: ProofUnconfirmed, canceller: None)
        let scope =
          Scope(
            ..scope,
            owners: put_owner(scope.owners, slot),
            verdict: worsen_for(scope.verdict, ProofUnconfirmed),
          )
        apply_proof(scope, slot)
      }

      // An absent owner's `noproc` `DOWN` was queued at adoption, ahead of
      // any grace, so the ordinary dispatch has already settled it by the
      // time this event can fire; the arm is totality.
      ProofAbsent -> scope
      ProofDrained -> scope
      ProofLost(..) -> scope
      ProofUnconfirmed -> scope
    }
  })
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
  // The caller's death ends the run: there is nobody to deliver to, so the
  // scope's remaining duty is to make sure nothing it spawned survives it —
  // and, for managed work, to keep witnessing until every owner has drained.
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
    // Already accounted, or not a worker at all.
    Error(Nil) -> reap_settled_exit(scope, pid, reason)
  }
}

/// An exit from something that owes no outcome: a worker that already
/// answered, or a cancel helper. Both are erased from the ledgers that keep
/// `settled` honest; a helper that crashed is logged, because a broken
/// `cancel` closure is a bug its author needs to see and must cost the run
/// nothing else.
fn reap_settled_exit(
  scope: Scope(a, e),
  pid: Pid,
  reason: ExitReason,
) -> Scope(a, e) {
  use <- bool.lazy_guard(when: !list.contains(scope.helpers, pid), return: fn() {
    Scope(
      ..scope,
      finishing: list.filter(scope.finishing, fn(other) { other != pid }),
    )
  })

  case reason {
    process.Abnormal(..) ->
      sys.warn("weft: a managed task's cancel closure crashed")
    process.Normal -> Nil
    process.Killed -> Nil
  }
  Scope(
    ..scope,
    helpers: list.filter(scope.helpers, fn(other) { other != pid }),
  )
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

/// Settle one task's worker fact against its ownership fact.
///
/// A plain task settles directly. A managed one settles only when its
/// owner's proof is in: a pending proof withholds the outcome, and a
/// resolved one seals it — the worker's own answer where the drain was
/// proven, `DrainProofLost` or `CancellationUnconfirmed` where it was not.
fn note_outcome(scope: Scope(a, e), outcome: Outcome(a, e)) -> Scope(a, e) {
  case proof_for(scope.owners, outcome.index) {
    None -> queue_outcome(scope, outcome)
    Some(ProofPending) ->
      Scope(..scope, awaiting: [#(outcome.index, outcome), ..scope.awaiting])
    Some(ProofAbsent) ->
      Scope(..scope, awaiting: [#(outcome.index, outcome), ..scope.awaiting])
    Some(ProofDrained) -> queue_outcome(scope, outcome)
    Some(ProofLost(reason:)) ->
      queue_outcome(scope, DrainProofLost(index: outcome.index, reason:))
    Some(ProofUnconfirmed) ->
      queue_outcome(scope, CancellationUnconfirmed(index: outcome.index))
  }
}

/// Seal a withheld outcome with the proof that just resolved. The pending
/// arm restores the stash — unreachable from `apply_proof`, which only
/// seals resolved proofs, but the type owns it.
fn seal_outcome(outcome: Outcome(a, e), proof: Proof) -> Outcome(a, e) {
  case proof {
    ProofPending -> outcome
    ProofAbsent -> outcome
    ProofDrained -> outcome
    ProofLost(reason:) -> DrainProofLost(index: outcome.index, reason:)
    ProofUnconfirmed -> CancellationUnconfirmed(index: outcome.index)
  }
}

/// Queue a sealed outcome for delivery and apply the failure policy to it.
fn queue_outcome(scope: Scope(a, e), outcome: Outcome(a, e)) -> Scope(a, e) {
  // A consumer that has halted or died is not owed an account, and queueing for
  // it would grow a backlog nobody will ever drain.
  let scope = case scope.consumer {
    Waiting -> Scope(..scope, ready: push_backlog(scope.ready, outcome))
    Busy -> Scope(..scope, ready: push_backlog(scope.ready, outcome))
    Halted -> scope
    Gone -> scope
  }

  // A lost proof ends the run the way a crash does: the caller asked for
  // fail-fast, and "we no longer know whether that task's work stopped" is
  // a failure by any reading. An unconfirmed cancellation does not — it can
  // only exist once cancellation has already begun.
  let ends_the_run = case outcome {
    Failed(..) -> True
    Crashed(..) -> True
    DrainProofLost(..) -> True
    Completed(..) -> False
    Abandoned(..) -> False
    NeverStarted(..) -> False
    CancellationUnconfirmed(..) -> False
  }
  use <- bool.guard(when: !ends_the_run, return: scope)

  case scope.on_failure {
    KeepGoing -> scope
    CancelSiblings -> begin_cancel(scope)
  }
}

/// The proof standing for a task's owner, or `None` for a plain task.
fn proof_for(owners: List(OwnerSlot), index: Int) -> Option(Proof) {
  case list.find(owners, fn(slot) { slot.index == index }) {
    Ok(slot) -> Some(slot.proof)
    Error(Nil) -> None
  }
}

/// The slot watching `pid`, if any.
fn find_owner(owners: List(OwnerSlot), pid: Pid) -> Result(OwnerSlot, Nil) {
  list.find(owners, fn(slot) { slot.pid == pid })
}

/// Replace a slot in the ledger, matching on its task index.
fn put_owner(owners: List(OwnerSlot), slot: OwnerSlot) -> List(OwnerSlot) {
  list.map(owners, fn(existing) {
    case existing.index == slot.index {
      True -> slot
      False -> existing
    }
  })
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

  // Owners are asked, never killed: each unresolved owner's `cancel` runs on
  // a disposable helper, and the grace — if one was configured — starts now,
  // one window for the whole teardown rather than one per task.
  let scope = arm_grace(dispatch_cancels(scope))

  // Never-started outcomes route through the ordinary settle so a managed
  // task that never ran still waits for — and is sealed by — its owner's
  // proof.
  let never_started =
    list.map(scope.pending, fn(slot) { NeverStarted(index: slot.0) })
  let scope = Scope(..scope, cancelling: True, pending: [])
  list.fold(never_started, scope, note_outcome)
}

/// Run every unresolved owner's `cancel` on its own disposable helper.
///
/// One helper per owner, spawned linked so nothing outlives the scope, and
/// tracked so the scope does not return while one runs. Disposable is the
/// point: a `cancel` closure that crashes costs the run a log line, never
/// its witness. Sending every ask before waiting on anything is the same
/// kill-then-join ordering the workers get.
fn dispatch_cancels(scope: Scope(a, e)) -> Scope(a, e) {
  list.fold(scope.owners, scope, fn(scope, slot) {
    case slot.proof, slot.canceller {
      ProofPending, None -> {
        let helper = process.spawn(slot.cancel)

        let slot = OwnerSlot(..slot, canceller: Some(helper))
        Scope(..scope, owners: put_owner(scope.owners, slot), helpers: [
          helper,
          ..scope.helpers
        ])
      }

      ProofPending, Some(..) -> scope
      ProofAbsent, _canceller -> scope
      ProofDrained, _canceller -> scope
      ProofLost(..), _canceller -> scope
      ProofUnconfirmed, _canceller -> scope
    }
  })
}

/// Arm the cancellation grace, if the run configured one.
fn arm_grace(scope: Scope(a, e)) -> Scope(a, e) {
  case scope.grace, scope.grace_timer {
    Some(milliseconds), None ->
      Scope(
        ..scope,
        grace_timer: Some(process.send_after(
          scope.grace_alarm,
          milliseconds,
          Nil,
        )),
      )

    Some(..), Some(..) -> scope
    None, _timer -> scope
  }
}

/// Say `Done`, drop the link, and let the process end carrying the verdict.
fn finish(scope: Scope(a, e)) -> Nil {
  flush_deadline(scope)
  flush_grace(scope)

  case scope.consumer {
    Gone -> Nil
    Waiting -> process.send(scope.outbox, Done)
    Busy -> process.send(scope.outbox, Done)
    Halted -> process.send(scope.outbox, Done)
  }

  // Unlinking from this side means no exit signal is ever generated, which is
  // what keeps a caller that traps exits from seeing the scope's ordinary
  // finish as unexpected mailbox noise — the verdict below travels by
  // monitor, never down this link. Doing it here, after the last thing that
  // could fail, leaves the link covering the entire run.
  process.unlink(scope.caller)

  // The exit reason is the run's drain verdict, which is what lets scopes
  // compose: an outer witness monitoring this pid learns "everything this
  // run started is provably gone" from a normal exit and nothing else, so a
  // lost or unconfirmed proof must exit loudly. `erlang:exit/1` raises, so
  // trapping does not soften it.
  case scope.verdict {
    AllDrained -> Nil
    SomeUnconfirmed -> stop_self("weft_drain_unconfirmed")
    SomeLost -> stop_self("weft_drain_proof_lost")
  }
}

/// Exit this process abnormally with `reason` as an atom.
fn stop_self(reason: String) -> Nil {
  let _never = exit_self(atom.create(reason))
  Nil
}

/// `erlang:exit/1`: raise an exit with the given reason. Never returns; the
/// stock BIF's shape lines up with a Gleam signature directly, so no shim
/// module is needed.
@external(erlang, "erlang", "exit")
fn exit_self(reason: atom.Atom) -> Bool

/// Cancel the grace timer, and drain its message if it beat us to it. The
/// same flush `flush_deadline` does, for the same reason.
fn flush_grace(scope: Scope(a, e)) -> Nil {
  case scope.grace_timer {
    None -> Nil
    Some(timer) ->
      case process.cancel_timer(timer) {
        process.Cancelled(_) -> Nil
        process.TimerNotFound -> {
          let _fired = process.receive(scope.grace_alarm, 0)
          Nil
        }
      }
  }
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
