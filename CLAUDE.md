# Weft

Weft is a Gleam library: owned, bounded structured concurrency (the `weft`
module) plus typed bindings for the OTP behaviours `gleam_otp` doesn't
cover (`weft/actor`, `weft/state_machine`, `weft/event_manager`). It is a
single hex-publishable package, not a monorepo; the vendored linter under
`tools/lint` is the one exception and is destined to spin out.

## Start here

**`docs/plan.md`** is the plan of record: the module dependency graph, who
owns which file, and the design rulings already made. Read it before
planning anything, and rewrite it when you finish a body of work.

The designs themselves live in the GitHub issues, one per module:
[#1](https://github.com/Roasbeef/weft/issues/1) the run engine,
[#2](https://github.com/Roasbeef/weft/issues/2) state_machine,
[#3](https://github.com/Roasbeef/weft/issues/3) event_manager,
[#4](https://github.com/Roasbeef/weft/issues/4) actor extensions. An issue
is the design doc of record for its module until the module's `////` prose
supersedes it; when measurement contradicts an issue's diagnosis, the
correction goes on the issue as a comment.

## Required reading

Weft follows loom's Gleam style in full. If you have a loom checkout
beside this repo, read `../loom/docs/gleam-style.md` before writing code,
particularly Part III (idioms: error handling, type design, actors, FFI).
The short form, which the lint partially enforces:

- Chain fallible steps with `use` + `result.try`; `case` is for ADT
  dispatch, never for stacking `Result`s, and never buy a shallower shape
  with a catch-all `_ ->`.
- No `panic` or bare `let assert` in `src/`. A `let assert` that survives
  review carries `as "why this cannot fail"`.
- Watch the eager ones: `bool.guard`'s `return:` and every `unwrap`
  fallback are ordinary arguments, computed on every call; anything that
  recurses or allocates belongs in the `lazy_*` form.
- Total decoders at every dynamic boundary. In this package that means
  the exit-reason and system-message edges.

## Literate code

Comments are part of the design, not decoration. Start every module with
`////` documentation explaining why the boundary exists and how work moves
through it. Document every public type, constructor field, variant, and
function with `///`, including an `## Examples` section for functions.
Write the reasoning the syntax cannot show: the invariant being preserved,
the failure or race that shaped the code, why this mechanism owns the
responsibility. Before a selector, recursive loop, or subtle `case` arm,
state what event or transition it represents. Treat missing explanatory
prose as unfinished work when reviewing a change.

## Ground rules

- Design priorities, in order: correctness, robustness, API taste,
  performance.
- Gleam >= 1.18, Erlang/OTP >= 29, erlang target only. All code passes
  `gleam format --check` and compiles warning-free before commit.
- The link topology in issue #1 is the load-bearing design: no task
  outlives its scope, no scope outlives its caller, enforced by link
  propagation rather than by any loop staying alive. Changes to it are
  design changes and go through the issue, not silent drift.
- `weft/internal/*` modules are internal (enforced by `gleam.toml`).
  Shared machinery (system message handling, timer bookkeeping) lives
  there once and is never written twice.
- Kill-then-join, in that order: when cancelling workers, send every kill
  first, then wait for every `EXIT`. Interleaving lets one slow exit
  postpone every later cancellation.

## Working in the repo

`make help` lists the common commands. `make check` is the full gate:
format check, warning-free build, tests, lint, doc-check. `make lint`
runs the vendored loom linter over `src/` (R0/R2/R4 gate, the rest warn;
read the warnings). `make doc-check` enforces the AGENTS.md mirror and
`////` module doc coverage.

Keep local changes to `tools/lint` at zero so the spin-out into its own
repo stays a `git mv`; if a rule needs changing, change it in loom first
and re-vendor.

`main` is the primary branch. Work happens on short-lived topic branches
named for the work itself (`engine/outcome-ordering`,
`statem/postpone-replay`), never for the tool or agent that produced it.

## Per-directory docs

`src/weft/CLAUDE.md` covers the module graph in detail: key types, the
actor/register/wire traffic between scope and workers, and the invariants
that break things when violated. `AGENTS.md` beside any CLAUDE.md is a
byte-identical mirror produced by `cp`, never hand-edited; `make
doc-check` enforces it. After changing a module's types, messages, or
dependencies, refresh the docs (the `/doc-gardening` skill in a loom
checkout is the model to follow).

## Commits

Incremental, atomic commits that each tell one part of the story, authored
by the repository owner (Olaoluwa Osuntokun <laolu32@gmail.com>), never by
a tool or agent identity, no AI co-author trailers. Format: `subsystem:
imperative summary under 50 chars`, then a body in natural prose
explaining the why more than the what. Prefixes: `engine:`, `actor:`,
`statem:`, `events:`, `internal:`, `docs:`, `build:`, `test:`. Vendored
code (`tools/lint`) gets its own commits.
