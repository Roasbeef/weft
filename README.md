# weft

Owned, bounded structured concurrency for Gleam, plus typed bindings for the
OTP behaviours the official bindings don't cover.

The name comes from weaving. The warp threads are the ones held under tension
on the loom, the long lived structure, which is what a supervision tree is.
The weft is the thread drawn across them, one transient pass at a time, which
is what a task is.

```sh
gleam add weft
```

## weft — the run engine

Fan a list of tasks out across a bounded pool of processes and get back a
complete account of what happened to every one of them: the ones that
succeeded, the ones that failed, the ones that crashed, the ones cancelled
mid flight, and the ones that never got a slot. Every task is owned by a
scope process, and the link topology enforces what the API claims: no task
outlives its scope, and no scope outlives its caller.

```gleam
import weft

let outcomes =
  urls
  |> list.map(fn(url) { fn() { fetch(url) } })
  |> weft.new
  |> weft.limit(8)
  |> weft.deadline(30_000)
  |> weft.start

let #(bodies, rest) = weft.partition(outcomes)
```

`start` returns outcomes in input order; `fold` streams them in completion
order with pull-based delivery, so a slow consumer throttles the run instead
of filling a mailbox. Sugar on top: `map`, `race`, `first_ok`. A run blocked
inside `start` can be stopped from any other process with a cancel signal.

For work that persists beyond its worker — an HTTP request whose socket
lives under a client library's own supervisor — a *managed* task publishes
an owner pid and a cancel capability alongside `begin`:

```gleam
let outcomes =
  weft.new_prepared([weft.prepared_task(owner:, cancel:, begin:)])
  |> weft.cancel_grace(2000)
  |> weft.start
```

The scope monitors every owner before any worker spawns, holds the task's
slot until both worker and owner have exited, and only a *normal* owner
exit proves the subtree drained: an abnormal one is `DrainProofLost`, and a
cancellation whose grace expires with the owner alive is
`CancellationUnconfirmed`. The scope's own exit reason carries the run's
drain verdict, so scopes compose: `start_detached` hands back a handle
(`pull`, `cancel_detached`, `scope_pid` — or `start_relayed` for push
delivery to an actor), and a nested detached scope is itself a publishable
owner. Scopes answer OTP system messages like every other weft process.

## weft/actor — the superset actor

A strict superset of `gleam/otp/actor`'s builder: the upstream surface works
unchanged (swap the import), plus the gen_server features upstream doesn't
carry. `continuing` closes the deferred-init race — `start` returns
immediately, and the continue message is guaranteed to be handled before
anything else in the mailbox:

```gleam
import weft/actor

actor.new_with_initialiser(1000, fn(subject) {
  actor.initialised(Empty)
  |> actor.returning(subject)
  |> actor.continuing(LoadIndex)
  |> Ok
})
|> actor.on_message(handle)
|> actor.hibernate_after(30_000)
|> actor.idle_timeout(3_600_000, ExpireSession)
|> actor.periodic(every: 30_000, sending: RenewLease)
|> actor.on_shutdown(fn(state, _reason) { store.close(state.store) })
|> actor.trapping_exits(True)
|> actor.start
```

Also: `then_handle` (gen_statem's `next_event` for plain actors),
hibernation, an idle timeout, a fixed-delay heartbeat, and a best-effort
shutdown callback.
`supervised` hands back `gleam_otp`'s own `ChildSpecification`, so a weft
actor drops into an upstream supervisor unchanged.

## weft/state_machine — a typed gen_statem

State ADTs with compiler-checked exhaustiveness, `postpone`, four kinds of
timeout, and enter callbacks. The two features that delete the most
hand-rolled code: postponing an event until the next state change, and a
state timeout that dies with the state that armed it.

```gleam
import weft/state_machine as sm

fn handle(state: State, data: Data, message: Message) -> sm.Next(State, Data, Message) {
  case state, message {
    // A request that arrives while connecting is re-queued by the machine
    // and redelivered the instant we reach Ready. No pending list.
    Connecting, Request(..) -> sm.keep(data) |> sm.postpone

    Connecting, Connected(handle) ->
      sm.transition(to: Ready, data: Data(..data, handle: Some(handle)))
      |> sm.with_event_timeout(60_000, IdleTooLong)

    // The Retry timer dies with the Backoff state, so a stale Retry can
    // never arrive after something else already moved us on.
    Connecting, ConnectFailed(_) ->
      sm.transition(to: Backoff, data:)
      |> sm.with_state_timeout(backoff_ms(data.attempts), Retry)

    // ...the compiler tells you which state/message pair you forgot.
  }
}
```

The fourth timeout kind is periodic: `with_periodic_timeout(name:, every:,
sending:)` is a named timeout that arms itself again once the handler for
each fire has returned, so a heartbeat keeps beating across every state the
machine moves through and stops when `cancel_timeout` says so. The cadence
is fixed delay rather than fixed rate, so a slow handler slows the ticks
down instead of building a backlog of them in the mailbox.

An enter callback returns `Enter`, not `Next`, so postponing where there is
no event in hand is a compile error rather than a surprise at runtime.

## weft/registry — reclaimable actor addresses

A long-lived server can give each restartable actor an `Address(message)`
without allocating a permanent process-name atom. The address stays the
same across restarts; `lookup` resolves its current subject from an unnamed
ETS table, and recipient monitors reclaim dead bindings.

```gleam
import weft/registry

let assert Ok(names) = registry.start()
let address = registry.new_address(names)
let assert Ok(started) =
  actor.new(0)
  |> actor.addressed(address)
  |> actor.start

assert registry.lookup(address) == Ok(started.data)
```

`state_machine.addressed` has the same registration-before-initialisation
contract. Resolve an address again when sending to a restarted recipient:
a cached subject still points to its old process. The linked registry owns
resolution only. Stop recipients and drain their effects before stopping
the registry; registry shutdown itself does neither.

## weft/event_manager — a typed gen_event

One process fanning events out to an ordered list of handlers, each carrying
its own private state. The heterogeneous handler list gen_event needs is
encoded by making each handler return its own successor, so state lives in a
closure and never reaches the type:

```gleam
import weft/event_manager as events

// One handler holds an Int, the other a file handle. Same list.
let assert Ok(bus) =
  events.new()
  |> events.add(events.handler(0, on_event: count_tokens))
  |> events.add(events.handler(open(path), on_event: append_transcript))
  |> events.start

events.notify(bus.data, TurnFinished(1, 4210))

// Wait until every handler has seen it: the backpressure variant.
events.sync_notify(bus.data, Flush, waiting: 1000)
```

A handler that fails is removed and logged without disturbing its siblings,
and the type forces it to say why. A handler that needs to be queried is not
a handler; it is an actor that happens to subscribe.

## weft/poll — bounded polling in the caller's own process

Some waits cannot be handed to another process: the waiter is the one that
needs the answer, the thing it waits on is a synchronous probe, and the only
honest bound is the wall clock. `poll.until` is that loop decided once — the
first attempt is immediate, a last attempt is made at the deadline, and a
probe that failed for good is told apart from one that merely has not
succeeded yet:

```gleam
import weft/poll

case
  poll.until(within: 5000, every: 25, attempt: fn() {
    case try_lock(path) {
      Ok(lock) -> poll.Done(lock)
      Error("busy") -> poll.Retry
      Error(reason) -> poll.Fail(reason)
    }
  })
{
  poll.Answered(lock) -> Ok(lock)
  poll.Failed(reason) -> Error("acquire lock: " <> reason)
  poll.Expired -> Error("timed out waiting for the lock")
}
```

It owns no process; a wait that could be a message should be a
`weft/state_machine` state with a timeout instead.

A wait that belongs to a system with its own injected time capability
cannot consult the operating system without either hanging that system's
simulation or making it non-deterministic, so the clock is a value:
`poll.Clock(now:, sleep:)`, `poll.monotonic()` for the one `until` uses,
and `poll.until_on` for the caller's own. `poll.fold_until` is the same
loop with the probe threading a state from one attempt to the next —
the handles already settled, the token the last exchange handed back —
and expiry gives that state back (`RanOut`) rather than only reporting
that time ran out. Both take an `Interval`, so a long wait can back off
(`Doubling(from: 25, to: 250)`) instead of probing flat.

## Relationship to gleam_otp

Weft is not a fork and not a competing framework. Its types interoperate
with `gleam_otp` directly: every module's `supervised` returns the upstream
`ChildSpecification`, `start` returns the upstream `StartResult`, and all
four processes answer OTP system messages, so they show up in the observer
and freeze correctly under `sys:suspend/1`. Weft covers the ground upstream
has (so far) chosen not to: transient fan-out with an ownership guarantee,
and the behaviours whose Erlang APIs don't survive typing without a
redesign.

## Development

`make help` lists the common commands. `make check` is the full gate:
format check, warning-free build, tests, the house lint, and the doc graph
check. The linter under `tools/lint` is borrowed from
[loom](https://github.com/Roasbeef/loom) and vendored for now.

## License

Apache-2.0. `weft/actor` derives from `gleam_otp`'s actor (also
Apache-2.0); see NOTICE.
