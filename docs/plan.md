# Plan of record

Rewritten at the close of the initial build-out (2026-08-31), and amended
as each phase landed. Six public modules are on `main` — the run engine,
`actor`, `state_machine`, `event_manager`, `poll` and `timer` — every gate
green: 163 tests, lint 0 errors, doc graph clean.

## What landed

| Module | Design | Landed | Notes |
|---|---|---|---|
| `weft` | [#1](https://github.com/Roasbeef/weft/issues/1) (open) | `6fa884b`..`92c8699` | Pull-based delivery; slot held until delivery, so `limit` bounds work plus unconsumed results |
| `weft/actor` | [#4](https://github.com/Roasbeef/weft/issues/4) (closed) | `3cf3436`..`3b70c62` | Superset of upstream; continues, `on_shutdown`, hibernation, idle timeout |
| `weft/state_machine` | [#2](https://github.com/Roasbeef/weft/issues/2) (closed) | `9da435e`..`a034557` | `Enter` vs `Next` via phantom marker on opaque `Step` |
| `weft/event_manager` | [#3](https://github.com/Roasbeef/weft/issues/3) (closed) | `48f8fbe`, `d2b2a54` | Built on `weft/actor`; no loop of its own |
| `weft` managed tasks | [#5](https://github.com/Roasbeef/weft/issues/5) (closed) | `d348e22`..`25cf4a0` | Owner ledger, drain proof, grace, detached runs, scope on the sys plane; `0.2.0` |
| `weft/poll` | loom#159 phase 2 | `0.4.0` | Bounded synchronous polling for foreground waits; owns no process |
| `weft` dynamic adoption | loom#159 phase 2 | `0.3.0`, `0.3.1` | `managed`/`Ledger`/`adopt`/`adopt_leaf`, `start_witnessed` (a `Witnessed` handle since 0.3.1), `cancel_when_exits`; many owners per task |
| Periodic timeouts | loom#159 phase 3 | `0.4.1` | `sm.with_periodic_timeout` and `actor.periodic`; fixed delay, re-armed after the handler |
| Injected clocks for `weft/poll` | loom#159 phase 3 | `0.4.1` | `Clock`/`monotonic`, `until_on`, `fold_until`, `Interval` |
| Injected timer source | loom#165 | `0.4.2` | `weft/timer`'s `Source`; `with_timer_source` on the machine and the actor |

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

Dynamic adoption (0.3.0) was the deferred item loom's custodian consumers
came back for, and it settled these:

- A task's proof is the *aggregate* over every owner it holds: lost as
  soon as any one is lost, unconfirmed if any is unconfirmed, pending
  while any is pending, drained only when all are. A task is sealed at
  most once (`Scope.sealed`), so a second owner resolving after a lost
  proof cannot write the account twice.
- A refused adoption still retains and asks the owner. The permit is what
  refusal withholds, never the witness.
- Owners adopted mid-run are judged by role even when already dead at
  adoption (`ProofPending`, not `ProofAbsent`): the caller asked us to
  witness something it had already started, and whether its death lost a
  subtree is exactly the role's question.
- A witnessed run (`Discarding` consumer) returns a slot the moment an
  outcome is sealed, since no delivery will ever return it, and sends no
  `Done`.

## Rulings made for periodic timeouts (0.4.1)

- **Fixed delay, not fixed rate.** The next fire is armed once the handler
  for this one has returned, so the interval measures the gap between the
  end of one tick and the start of the next. A handler slower than its own
  interval slows the ticks down rather than accumulating a backlog of them
  in the mailbox, which is what every consumer that asked for this wanted:
  two heartbeats delivered back to back say nothing a single one did not.
  A caller who needs fires on a grid wants a schedule, and a schedule is a
  different primitive this deliberately is not.
- **A periodic timeout is a named timeout that re-arms itself**, sharing
  the name space with `with_named_timeout`. One name is one timer,
  `cancel_timeout` ends either kind, and arming a name the other way round
  converts it. The alternative — a fourth key space with a `cancel_periodic`
  beside `cancel_timeout` — buys a machine the ability to hold a one-shot
  and a periodic under the same name, which is a confusion rather than a
  capability.
- **The re-arm runs after the step's own timer actions and before the enter
  callback.** That is what makes the handler's word final: a handler that
  cancels the name ends the series even though its own fire is the one
  being handled, and a handler that arms a new interval gets the new one.
  It is also what makes a cancel from inside a tick safe — the fire already
  in the mailbox carries a stale generation stamp and dies in the timer
  book like any other.
- **The actor's is a builder setting, not a step action.** `actor.periodic`
  sits beside `idle_timeout` because the actor has no per-step timer
  surface at all and growing one would be growing a state machine. A
  consumer that needs to re-time or cancel a tick is a
  `weft/state_machine`, where the interval belongs to the step. What an
  actor handler can still do is `stop`.
- **A beat resets the loop timeout**, because by the time the handler sees
  it a beat is an ordinary message and `idle_timeout` documents itself as
  reset by every message. gen_statem's event timeout has the same rule for
  the same reason: any event, including a timeout firing, ends the quiet.
  An actor wired with both wants that interaction on purpose.
- **Suspension freezes a periodic timeout like every other**, re-arming
  from full on resume rather than delivering the ticks the frozen process
  could not have acted on.

## Rulings made for the injected poll clock (0.4.1)

- **The clock and the sleep are one value.** A wait that read a simulated
  clock and rested on the real one would burn wall time to make no logical
  progress; one that read the real clock and rested by stepping a simulated
  one would never end. `Clock(now:, sleep:)` makes them a single decision,
  and `monotonic()` is the pair `until` has always used.
- **`until` keeps its exact signature.** It is published and consumed, and
  the injected form is an addition rather than a generalisation callers have
  to be migrated through. It is now written as `until_on(monotonic(), ...)`,
  so there is one loop and not two to keep honest.
- **`fold_until` is the engine and `until_on` the special case** where the
  carried state is `Nil`. The consumer that asked for this was a wait
  accumulating which of its handles had already settled, and a
  `fn() -> Attempt` cannot express that without a process or a mutable cell.
- **Expiry hands the state back** (`RanOut`) rather than reporting only that
  time ran out. A wait that half-succeeded should not have to start again to
  find out what it got, and the caller who wanted the old behaviour still
  has `Expired` on the stateless form.
- **The interval is a value, not a number**, because the waits that need an
  injected clock are the long ones and a long wait probing at a short flat
  interval is thousands of reads. `Doubling(from:, to:)` is the cheapest
  schedule that fixes that without a parameter nobody can pick.
- **A clock whose `now` never moves is a wait that never expires**, and that
  is documented as a fact about the clock rather than defended against. A
  loop that also subtracted its own nap from the budget would terminate on a
  frozen clock, but it would then be measuring two different times at once
  and reporting neither.

## Rulings made for the injected timer source (0.4.2)

- **A two-variant `Source`, not a closure on its own.** A book that took
  only `fn(delay_ms, wake) -> Nil` would have to arm the wall clock through
  that shape too, and the wall clock's arming call returns something the
  book uses: `process.send_after` yields a handle, and `cancel` stops the
  superseded timer with it. Flattening both into one closure would either
  throw that handle away — making every weft loop pay the injected source's
  uncancellability, on the default path, forever — or push a handle type
  into the closure's return and make a fake wheel implement it. `WallClock`
  and `Injected` are the two arming stories weft actually has, and the
  book's `set` is the one place that tells them apart.
- **The injected shape mirrors the consumer's own seam exactly.** The
  consumer that asked for this holds its time capability as
  `fn(Int, fn() -> Nil) -> Nil` — one call, no handle, no result — and
  passes it straight in. A shape of weft's own devising, however slightly
  better, would be an adapter at every call site and a second place for the
  contract to drift. It is also the weakest shape that can work, which is
  what lets a fake wheel in a test be a legal source.
- **Cancel is best effort under injection, and that is sound rather than
  tolerated.** The book has always had to recognise a fire it could not
  stop: `erlang:cancel_timer` cannot recall a message already in the
  mailbox, so `accept` compares generations and drops the stale one. An
  injected arming is that window widened to always. Nothing new defends it
  because nothing new is needed — and the corollary is that the generation
  check is now load-bearing rather than belt-and-braces, which is why the
  new tests run a cancelled arming's wake on purpose rather than racing it.
- **The source is fixed at construction, not passed per timer.** It is a
  property of the loop: a machine arming half its timeouts on a logical
  clock and half on the wall clock has no coherent notion of when anything
  happens, and no consumer has asked for one. `new_on` takes it once, the
  builders store it, and every rule already written about the four timeout
  kinds — reset, cancel, flush, the periodic re-arm after the handler,
  disarm-on-suspend and re-arm-from-full on resume — holds unchanged on
  either source because all of them go through `set`.
- **What was cut.** No `Clock(now, sleep)` source: the book needs `after`
  and nothing else, and it never reads a clock or rests. No change to
  `weft/poll`, whose injected `Clock` answers a different question (a
  foreground wait in the caller's own process) and is not the same value.
  No source on the run engine's `deadline`, which no consumer has asked
  for. And no way to ask a book which source it is on: a loop knows, and a
  consumer that reads it is about to write a branch that should have been
  two configurations.

## Reclaimable addresses (single-daemon consumer)

Loom's shared daemon must open and close sessions without accumulating
permanent process-name atoms. `weft/registry` provides a typed reference
address whose current subject can change after a recipient exits. One
linked OTP owner holds an unnamed ETS table; readers resolve directly,
while registration and monitor cleanup run through that owner. The table
contains only bound addresses, not every address ever allocated.

The actor and state-machine builders gain `addressed`, alongside `named`.
Both configure one subject factory, so only the last setter applies.
Registration precedes custom initialisation and startup acknowledgement.
A failed initialiser exits and its monitor releases the binding. A caller
may also register a local unnamed subject explicitly, including one owned
by a child process.

The important boundary is ownership: the registry owns address resolution,
not recipient lifetime or effect drain. Replacing a dead binding is safe
for routing, but does not authorize restarting external work whose drain
has not been proved. Its caller must retain the namespace until all users
that need it have drained. Stopping the registry invalidates its addresses
without killing recipients. No unregister, distributed registration, or
application-message forwarding is needed by this consumer.

Each binding has a fresh monitor. An old DOWN cannot erase a replacement,
and repeated registration of the same subject adds neither rows nor
monitors. A lookup rejects a dead recipient even before cleanup runs. Tests
exercise replacement, stale DOWN replay, duplicate/conflicting bindings,
startup ordering and failure, namespace shutdown, and warmed lifecycle
cycles with no atom growth. A suspended-owner test checks that registration
timeouts release the surviving caller's monitor. The local suite contains
176 tests; this addition is not yet published.

The new dependency edges are `actor`/`state_machine` → `registry` →
`internal/registry`. The owner uses the existing `gleam/otp/actor` binding,
not a new receive loop. Registration policy and monitor cleanup stay in
Gleam; Erlang supplies only the missing ETS operations and local-subject
validation. Using the upstream actor here avoids a cycle through `weft/actor`'s
addressed builder without adding another public module.

## Deferred, deliberately

- **Detached start** and **the scope answering system messages** — both
  landed with #5 (`start_detached`/`pull`/`start_relayed`; the scope now
  answers get_state/get_status/suspend/resume and serves only the system
  plane while suspended).
- ~~Dynamic mid-run adoption~~ — landed in 0.3.0 (`managed`/`adopt`), once
  loom's custodian consumers showed composition was not enough: a nested
  scope treats its holder's normal exit as death and cancels, so a worker
  that returns while its children drain cannot be expressed by nesting.
- ~~A periodic timeout kind~~ — landed in `0.4.1` on both the machine and
  the actor, once loom's adoption survey came back with three consumers
  rather than one (the broker helper's heartbeat, the SQLite writer's
  lease renewal, the strand driver's poll tick).
- **Cancelling a state or event timeout while staying put**
  (gen_statem's `infinity`) — shape sketched on #2, waits for a consumer.
- **`on_handler_exit` / handler refs** for the event manager — waits for
  a real consumer (#3 comments).
- **`code_change`**, long-lived pools, nested-run discovery — #1 open
  questions 2 and 4, plus the hot-upgrade story; all post-1.0 material.

## Publishing

`0.1.0` and `0.2.0` are on hex.pm (published 2026-09-01). `0.2.0` carries
managed tasks (#5) and is an additive minor: two new `Outcome` variants
(`DrainProofLost`, `CancellationUnconfirmed`), so a consumer matching
exhaustively on `Outcome` gains two arms and nothing else moves. `0.3.0`
adds dynamic adoption, witnessed runs and `cancel_when_exits`, all
additive: no existing signature or variant changes. `0.3.1` changes one
signature published hours earlier: `start_witnessed` returns a
`Witnessed` handle (`witness_pid`, `cancel_witnessed`) rather than a bare
pid, because a witness-only caller still has to be able to cancel without
paying a signal process per run. The rest of loom's phase 2 developed
against a **path dependency** on this checkout rather than a hex release
per change, and `0.4.0` is that settled surface: `adopt_under` /
`adopt_leaf_under`, a pid that may be both watched and adopted, `unlinked`
start on the actor and machine, `with_selector` on a machine step, and
`weft/poll`. Every addition since 0.3.1 is additive.
`0.4.1` is loom's phase-3 pair, developed against a **path dependency**
on this checkout and cut once both had settled: the periodic timeout kind
on the machine (`with_periodic_timeout`) and the actor (`actor.periodic`),
and the injected clock for `weft/poll` (`Clock`, `monotonic`, `until_on`,
`fold_until`, `Interval`). Both are additive — no existing signature,
variant or behaviour moves, and `until` still means exactly what it meant.
`0.4.2` adds the injected timer source: the public `weft/timer` module
with `Source`, `new_on` inside the book, and `with_timer_source` on the
actor and machine builders. Additive again — a builder that says nothing
gets `WallClock`, which is what `new` has always armed — and cut once
loom's schedule-scan consumer had a named timeout it could drive on its own
time base rather than a hand-rolled generation-stamped tick of its own.
`gleam publish` from the root builds, uploads, and pushes hexdocs in one
step (needs a hex account and API key). `1.0.0` is the API freeze and
should wait for the deferred engine follow-ups to settle the `weft`
module's surface.

## Verification standard (unchanged)

`make check` from the root is the gate CI would run: format check,
warning-free build, all tests, the vendored lint (errors gate, warnings
are read), the doc graph check. Verify a gate by its own exit code. New
public functions carry `## Examples`; new modules carry a `////` header
with a worked example; `src/weft/CLAUDE.md` and its `AGENTS.md` mirror are
refreshed when types, messages, or dependency edges move.
