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

## Deferred, deliberately

- **Detached start** for the engine (outcomes to a `Subject`, so an actor
  can drive a run without blocking its mailbox) — ruled in on #1, not yet
  built. First engine follow-up.
- **The scope answering system messages** — `internal/sys` exists now;
  wiring it into the engine's scope loop is small and makes runs visible
  in the observer. Second engine follow-up.
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
