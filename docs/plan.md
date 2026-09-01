# Plan of record

This is the handoff: the module graph, who owns which file, the design
rulings already made, and what is deliberately deferred. Rewrite it when a
body of work lands; it is worth more than any status comment.

## The modules and their designs

| Module | Design | What it is |
|---|---|---|
| `weft` | [#1](https://github.com/Roasbeef/weft/issues/1) | Run engine: bounded fan-out, scope ownership, total `Outcome` accounting |
| `weft/actor` | [#4](https://github.com/Roasbeef/weft/issues/4) | Superset of `gleam/otp/actor`: `continuing`/`then_handle`, `on_shutdown`, hibernation, idle timeout |
| `weft/state_machine` | [#2](https://github.com/Roasbeef/weft/issues/2) | Typed gen_statem: postpone, state/event/named timeouts, enter callbacks |
| `weft/event_manager` | [#3](https://github.com/Roasbeef/weft/issues/3) | Typed gen_event: successor-closure handlers, drop-and-log isolation |
| `weft/internal/sys` | — | OTP system message selection and replies, written once |
| `weft/internal/timer` | — | Named-timer bookkeeping with cancel-and-flush, written once |

The issue is the design doc of record for its module until the module's
`////` prose supersedes it. The comment thread on #1 carries the rulings
made after the initial design (detached start, pull-based `fold` delivery,
loser cancellation for `race`/`first_ok`, the `Abandoned` vs third-party
kill rule, timer flush, `Outcome` helpers, trap-exits noise).

## Dependency graph

```
gleam_erlang/process
        │
        ├── weft                    (engine; scope + workers on raw
        │                            primitives, trap_exits in the scope)
        │
        └── weft/internal/sys ──┐
            weft/internal/timer ┤
                                │
                    weft/actor ─┼── weft/event_manager
                                │      (an actor whose state is the
                                │       handler list)
                                └── weft/state_machine
                                       (own receive loop; shares the
                                        internals, not the actor loop)
```

Rulings behind the shape:

- **`weft` does not depend on `weft/actor`.** The scope process traps
  exits and juggles worker EXITs, kill-then-join cancellation, and slot
  scheduling; forcing that through an actor loop built for one-message-in,
  one-Next-out would contort both. It shares only the internals.
- **`weft/state_machine` owns its loop.** Postpone replay and the
  three-timeout discipline are the loop; wrapping `weft/actor` would mean
  the actor loop growing statem features it shouldn't have.
- **`weft/event_manager` builds on `weft/actor`.** It is exactly an actor
  whose state is `List(Handler(event))`; it should prove the actor's
  builder is good enough to build on.
- **System message handling and timer bookkeeping are written once**, in
  `weft/internal/*`, because the flush-after-cancel race and the
  suspend/resume protocol are precisely the code that drifts when copied.

## Work packages and ownership

One package per worker; a worker touches only its own files. Exact
internal signatures are settled by the package that owns them, and later
packages build against what actually landed, not against this file.

**WP-E (engine)** — issue #1, milestones M1 through M4, plus the #1
comment rulings that bear on the core: pull-based `fold`, loser
cancellation, the `Abandoned` rule, timer flush, `partition`/`values`/
`failures` helpers, and `fold_until`-style early halt. Detached start and
scope sys support are deferred (below).
Owns: `src/weft.gleam`, `test/weft_test.gleam`.

**WP-A (actor + internals)** — issue #4, plus `on_shutdown`,
`hibernate_after`, `idle_timeout` from the #1 comment thread. Owns the
two internal modules and any FFI they need.
Owns: `src/weft/actor.gleam`, `src/weft/internal/sys.gleam`,
`src/weft/internal/timer.gleam`, `src/*.erl` FFI,
`test/weft_actor_test.gleam`.

**WP-S (state_machine)** — issue #2. Starts after WP-A lands; consumes
`internal/sys` and `internal/timer` as found.
Owns: `src/weft/state_machine.gleam`, `test/weft_state_machine_test.gleam`.

**WP-V (event_manager)** — issue #3. Starts after WP-A lands; builds on
`weft/actor`.
Owns: `src/weft/event_manager.gleam`, `test/weft_event_manager_test.gleam`.

**Waves:** WP-E and WP-A run in parallel (wave 1); WP-S and WP-V run in
parallel after WP-A merges (wave 2); review, doc gardening, and
`src/weft/CLAUDE.md` close it out.

## Deferred, deliberately

- **Detached start** (`start` variants delivering outcomes to a
  `Subject`): ruled in on the #1 thread, lands after the engine core so
  the blocking path is proven first.
- **Scope answering system messages**: wants `internal/sys`, which WP-A
  owns; wiring it into the engine is a small follow-up once both exist.
- **`code_change` and hot upgrade** for `weft/actor`: fits Gleam's
  total-decoder grain well, but nobody needs it yet.
- **Long lived pools and nested-run discovery**: open questions 2 and 4
  on #1, deferred past all of the above.

## Verification standard

Every package passes, from the repo root: `gleam format --check src
test`, `gleam build --warnings-as-errors`, `gleam test`,
`scripts/lint.sh` (zero errors; read the warnings), and
`scripts/doc_check.sh`. Property-style tests are named in each issue's
exit criteria; a package is not done without them, and not done while any
public function lacks an `## Examples` block.
