# src/weft — the module graph

Four public modules and two internal ones. The parent directory's
`weft.gleam` (the run engine) is documented here too, since the graph only
makes sense whole.

## The modules

- **`weft`** (in `src/`, not this directory) — the run engine. One scope
  process per run, trapping exits, workers linked to it; delivery to the
  caller is pull-based (`Delivered` carries the scope's inbox, caller
  answers `Next`/`Stop`). Managed tasks (`prepared_task`/`prepared_leaf`)
  add an owner ledger: every published owner is monitored before the first
  worker spawns, a managed slot is held until worker AND owner have exited,
  and the scope's own exit reason carries the run's drain verdict (normal =
  every proof landed; `weft_drain_proof_lost` / `weft_drain_unconfirmed`
  otherwise). Detached runs (`start_detached`/`pull`/`cancel_detached`/
  `start_relayed`) reuse the same pull protocol behind a handle. A
  `managed` task's `begin` receives a `Ledger`, and `adopt`/`adopt_leaf`
  on it publish owners while the run is live (a task may hold many; its
  proof is the aggregate), and `adopt_under`/`adopt_leaf_under` stage a
  child beneath a parent owner, asked only once the parent has exited; `start_witnessed` runs with no consumer at all,
  the scope's exit being the whole report, behind a `Witnessed` handle
  (`witness_pid`, `cancel_witnessed`); `cancel_when_exits` names a
  consumer whose death cancels. Depends on
  `gleam_erlang/process` and `internal/sys` (the scope answers the system
  plane); does NOT use `weft/actor` — the scope's kill-then-join teardown
  and slot scheduling would contort an actor loop.
- **`weft/actor`** — the superset actor. Owns the house receive loop:
  injected-message queue (continues), the timer book for its idle timeout,
  the sys plane, hibernation. Reuses `gleam_otp`'s `Started`/`StartError`/
  `ChildSpecification` for interop — a weft actor slots into an upstream
  supervisor unchanged.
- **`weft/state_machine`** — typed gen_statem. Its own loop (a sibling of
  the actor's, not a wrapper): postpone replay, the three-timeout
  discipline, enter callbacks. Consumes both internals directly.
- **`weft/event_manager`** — typed gen_event, built ON `weft/actor` (its
  state is the handler list; it has no loop of its own). Reaches into
  `weft/internal/sys` for exactly one thing: `warn`, so a dropped handler
  leaves a trace.
- **`with_selector` on a machine step** — replaces the selector the loop
  receives with from the next receive on, so a handler can widen the
  mailbox to a channel it just created (a subject, a monitor) instead of
  forcing that phase into the initialiser and the start into a trampoline.
  Same contract as the actor's `with_selector`: replace, not add.
- **Linkage** — `start` links by default, as OTP does; `unlinked` on the
  actor and machine builders spawns without the link, for a process that
  must neither die with its starter nor take it down (a guard started by
  the consumer it serves). `supervised` ignores it: a supervisor always
  links its children.
- **`weft/poll`** — bounded synchronous polling in the caller's own
  process (`until` with an immediate first attempt, a last attempt at the
  deadline, `Fail` distinct from `Retry`). Owns no process; depends only
  on `gleam_erlang/process` for the sleep and a monotonic-clock external.
- **`weft/internal/timer`** — the named-timer book: generation-stamped
  fires, `accept` as the only consumption path. Cancel-with-flush is two
  halves: `cancel` stops what it can, `accept` drops what it could not.
- **`weft/internal/sys`** + `src/weft_sys_ffi.erl` — the system-message
  plane (`{system, From, Request}` selection, get_state/get_status/
  suspend/resume replies, observer status shape), `hibernate`, `warn`. All
  of the library's `@external` lives here and in the engine's one
  `system_info` call.

## Message traffic, concretely

- Engine: caller ⇄ scope via `Reply(a, e)` (`Ready`/`Delivered`/`Done`)
  and `Request` (`Next`/`Stop`/`CancelRun`/`Publish`, the last answered
  with an `Adoption` from inside the scope's step); workers → scope via
  `Report(worker, index, result)` then their linked `EXIT`; cancel signals
  and managed owners are watched by monitor (one generic monitor arm,
  discriminated by pid); each owner's `cancel` runs on a disposable linked
  helper whose `EXIT` is bookkeeping, not an outcome.
- Actor: one user-typed `message` per actor; system messages arrive as
  `sys.Incoming` via a record selector, timers as `timer.Fired(TimerKey,
  message)`.
- State machine: same shape as the actor, with `TimerKey` covering
  `StateTimeout`/`EventTimeout`/`Named(String)`.
- Event manager: opaque `Message(event)` — `Notify`/`SyncNotify`/
  `AddHandler`/`CountHandlers` — carried by a plain weft actor.

## Invariants that break things when violated

1. **Every `timer.Fired` goes through `timer.accept`.** A loop that
   selects fires straight into its handler has the stale-timer bug back.
2. **`sys.selecting` merges LAST into any selector.** A later arm can
   shadow it, and a loop invisible to `sys` is a debugging dead end.
3. **`sys.handle` sends the sys reply itself.** Replying again from a loop
   double-answers a blocked debug tool.
4. **Suspension freezes everything but the debug plane and parent exit** —
   mailbox, injected queues, postponed replay, and timers (disarm on
   suspend, re-arm from full on resume).
5. **Kill-then-join, in that order** (engine `begin_cancel`): all kills
   sent before any `EXIT` is awaited. Owners are the exception by design:
   they are *asked* (their `cancel`, on a helper) and never killed —
   killing the owner destroys the one process whose exit still proves
   anything.
6. **A managed outcome is sealed by its proof at exactly one place**
   (`note_outcome`/`seal_outcome`): worker facts and owner facts meet
   there, and `settled` refuses to end the run while any owner is
   `ProofPending` or `ProofAbsent` or any helper is alive. Delivering an
   outcome whose owner has not resolved re-opens the ownership hole weft#5
   closed. With many owners per task the proof consulted is the aggregate
   (`aggregate_proof`), and `Scope.sealed` guarantees a task is written to
   the account at most once however many owners resolve after it.
7. **A refused adoption still retains the owner** (`adopt_published`):
   refusal withholds the permit to begin new work, never the witness, so
   an owner published after cancellation is monitored, asked to stop, and
   waited for like any other.
8. **Depth-first injection everywhere**: what a handler injects runs
   before what was already queued; in the state machine the full order
   after a transition is enter-injected, then handler-injected, then
   replayed postponed events (arrival order), then the mailbox.
9. **The engine's scope unlinks the caller itself in `finish`** — moving
   that unlink, or doing it caller-side, reintroduces exit-message noise
   for trapping callers; the drain verdict travels by monitor (the
   `erlang:exit/1` after the unlink), never down the link.
10. **`weft/internal/*` stays internal** (`gleam.toml` seals it) and all
   FFI stays inside it; the exceptions are the engine's
   `erlang:system_info(schedulers_online)` and `erlang:exit/1` externals,
   both stock BIFs argued in place.

## Dependency edges (enforced by review, not tooling)

```
weft ──────────────► gleam_erlang/process, internal/sys
weft/actor ────────► internal/{sys,timer}, gleam_otp (types only)
weft/state_machine ► internal/{sys,timer}, gleam_otp (types only)
weft/event_manager ► weft/actor, internal/sys (warn ONLY)
weft/poll ─────────► gleam_erlang/process (sleep only)
```

Adding an edge not in this picture is a design change: it goes through
`docs/plan.md` and a reviewer, not a quiet import.
