# src/weft — the module graph

Four public modules and two internal ones. The parent directory's
`weft.gleam` (the run engine) is documented here too, since the graph only
makes sense whole.

## The modules

- **`weft`** (in `src/`, not this directory) — the run engine. One scope
  process per run, trapping exits, workers linked to it; delivery to the
  caller is pull-based (`Delivered` carries the scope's inbox, caller
  answers `Next`/`Stop`). Depends only on `gleam_erlang/process`; does NOT
  use `weft/actor` or the internals — the scope's kill-then-join teardown
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
- **`weft/internal/timer`** — the named-timer book: generation-stamped
  fires, `accept` as the only consumption path. Cancel-with-flush is two
  halves: `cancel` stops what it can, `accept` drops what it could not.
- **`weft/internal/sys`** + `src/weft_sys_ffi.erl` — the system-message
  plane (`{system, From, Request}` selection, get_state/get_status/
  suspend/resume replies, observer status shape), `hibernate`, `warn`. All
  of the library's `@external` lives here and in the engine's one
  `system_info` call.

## Message traffic, concretely

- Engine: caller ⇄ scope via `Reply(a, e)` (`Delivered`/`Done`) and
  `Request` (`Next`/`Stop`); workers → scope via `Report(worker, index,
  result)` then their linked `EXIT`; cancel signals are watched by monitor.
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
   sent before any `EXIT` is awaited.
6. **Depth-first injection everywhere**: what a handler injects runs
   before what was already queued; in the state machine the full order
   after a transition is enter-injected, then handler-injected, then
   replayed postponed events (arrival order), then the mailbox.
7. **The engine's scope unlinks the caller itself in `finish`** — moving
   that unlink, or doing it caller-side, reintroduces exit-message noise
   for trapping callers.
8. **`weft/internal/*` stays internal** (`gleam.toml` seals it) and all
   FFI stays inside it; the one exception is the engine's
   `erlang:system_info(schedulers_online)` external, argued in place.

## Dependency edges (enforced by review, not tooling)

```
weft ──────────────► gleam_erlang/process
weft/actor ────────► internal/{sys,timer}, gleam_otp (types only)
weft/state_machine ► internal/{sys,timer}, gleam_otp (types only)
weft/event_manager ► weft/actor, internal/sys (warn ONLY)
```

Adding an edge not in this picture is a design change: it goes through
`docs/plan.md` and a reviewer, not a quiet import.
