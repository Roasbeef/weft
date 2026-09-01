# Plan of record

Rewritten at the close of the initial build-out (2026-08-31). All four
public modules are on `main`, every gate green: 96 tests, lint 0 errors and
0 warnings, doc graph clean.

## What landed

| Module | Design | Landed | Notes |
|---|---|---|---|
| `weft` | [#1](https://github.com/Roasbeef/weft/issues/1) (open) | `6fa884b`..`92c8699` | Pull-based delivery; slot held until delivery, so `limit` bounds work plus unconsumed results |
| `weft/actor` | [#4](https://github.com/Roasbeef/weft/issues/4) (closed) | `3cf3436`..`3b70c62` | Superset of upstream; continues, `on_shutdown`, hibernation, idle timeout |
| `weft/state_machine` | [#2](https://github.com/Roasbeef/weft/issues/2) (closed) | `9da435e`..`a034557` | `Enter` vs `Next` via phantom marker on opaque `Step` |
| `weft/event_manager` | [#3](https://github.com/Roasbeef/weft/issues/3) (closed) | `48f8fbe`, `d2b2a54` | Built on `weft/actor`; no loop of its own |
| `weft` managed tasks | [#5](https://github.com/Roasbeef/weft/issues/5) (open) | `engine/managed-tasks` | Owner ledger, drain proof, grace, detached runs, scope on the sys plane |

`src/weft/CLAUDE.md` carries the module graph, the message traffic, and
the invariants; the closed issues carry the deviations from their own
specs, recorded as comments (the CallError correction on #3, the marker
type on #2). Where this file and the code disagree, measure the code.

## Rulings made during the build (beyond the issues)

- Engine delivery is pull, not push; the ack doubles as slot release.
- `race(first, rest)` — the empty race is unrepresentable.
- Loser cancellation for `race`/`first_ok` rides on `fold`+`Halt`; no
  third scope-side policy.
- A `Killed` worker exit is `Abandoned` only after the scope initiated
  cancellation; before that it is `Crashed(Killed)`.
- The scope unlinks the caller itself in `finish` (caller-side unlink
  cannot flush an already-queued `EXIT`).
- `sync_notify`/`count_handlers` inherit `call`'s crash-on-timeout;
  `gleam_erlang` 1.x has no `try_call` to build a `Result` on.
- Suspension disarms timers, resume re-arms from full — a documented
  departure from gen_statem, whose timers run through a freeze.

Managed tasks (#5) added their own rulings, recorded on the issue and in
the engine's module doc:

- Owners are adopted (monitored) before the first worker spawns, plus an
  `is_alive` check at adoption — the queued `noproc` `DOWN` alone cannot
  beat `fill_slots`, so the check is what holds `begin` back for an owner
  that was dead on arrival, and the `DOWN` still carries the real reason.
- Owners are asked, never killed; each `cancel` runs on a disposable
  linked helper, and a crashed helper costs a log line, never the witness.
  The helper is dismissed the moment its owner exits, so a `cancel` that
  blocks cannot hold the scope open past the fact it was asking for.
- Only a normal transitive-owner exit proves drain; `prepared_leaf` is the
  declared exemption for owners with no descendants. `noproc` counts as
  lost *whatever the role* — proof that was never on file was never proof,
  and the leaf exemption covers a crash of work that ran, not an owner that
  was a corpse before `begin` was admitted. The slot records that absence
  (`ProofAbsent`) so the task still lands in the account.
- A lost proof ends the run under `CancelSiblings`, exactly as a crash
  does; `CancellationUnconfirmed` never does, since it presupposes a
  cancellation already in flight.
- One `cancel_grace` window per teardown, not per task; the grace bounds
  the wait and pays for it by downgrading the scope's exit verdict.
- The scope's exit reason IS the drain verdict (`weft_drain_proof_lost` /
  `weft_drain_unconfirmed`), sent after the caller unlink so it travels by
  monitor only. That is the composition story: a detached scope handed to
  `prepared_task` as an owner propagates loss with no translation code.
- Detached demand is unary and idempotent (`Next` while `Waiting` is a
  no-op), which is what lets `pull` re-grant demand it cannot remember
  granting; `Ready` is the scope's first word so the handle has an inbox
  before the first delivery.

## Deferred, deliberately

- **Detached start** and **the scope answering system messages** — both
  landed with #5 (`start_detached`/`pull`/`start_relayed`; the scope now
  answers get_state/get_status/suspend/resume and serves only the system
  plane while suspended).
- **Dynamic mid-run adoption** (publishing an owner to a scope after the
  run started) — #5 ships composition instead: a nested detached scope is
  a publishable owner, and loom's custodian-shaped consumers should say
  whether that is enough before a mutable ledger is added.
- **A periodic timeout kind** for `weft/state_machine` (the heartbeat
  shape loom's broker helper needs: fire every N ms regardless of
  activity) — flagged by loom's adoption survey, waits for that consumer.
- **Cancelling a state or event timeout while staying put**
  (gen_statem's `infinity`) — shape sketched on #2, waits for a consumer.
- **`on_handler_exit` / handler refs** for the event manager — waits for
  a real consumer (#3 comments).
- **`code_change`**, long-lived pools, nested-run discovery — #1 open
  questions 2 and 4, plus the hot-upgrade story; all post-1.0 material.

## Publishing

The name `weft` is free on hex.pm (checked 2026-08-31). Checklist:
`LICENCE` and `NOTICE` are in the tree; `gleam publish` from the root
builds, uploads, and pushes hexdocs in one step (needs a hex account and
API key). Publish `0.1.0` first; `1.0.0` is the API freeze and should wait
for the deferred engine follow-ups to settle the `weft` module's surface.

## Verification standard (unchanged)

`make check` from the root is the gate CI would run: format check,
warning-free build, all tests, the vendored lint (errors gate, warnings
are read), the doc graph check. Verify a gate by its own exit code. New
public functions carry `## Examples`; new modules carry a `////` header
with a worked example; `src/weft/CLAUDE.md` and its `AGENTS.md` mirror are
refreshed when types, messages, or dependency edges move.
