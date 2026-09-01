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

Four modules. Each one's full design rationale lives in its module
documentation; the issues that shaped them are
[#1](https://github.com/Roasbeef/weft/issues/1),
[#2](https://github.com/Roasbeef/weft/issues/2),
[#3](https://github.com/Roasbeef/weft/issues/3) and
[#4](https://github.com/Roasbeef/weft/issues/4).

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
|> actor.on_shutdown(fn(state, _reason) { store.close(state.store) })
|> actor.trapping_exits(True)
|> actor.start
```

Also: `then_handle` (gen_statem's `next_event` for plain actors),
hibernation, an idle timeout, and a best-effort shutdown callback.
`supervised` hands back `gleam_otp`'s own `ChildSpecification`, so a weft
actor drops into an upstream supervisor unchanged.

## weft/state_machine — a typed gen_statem

State ADTs with compiler-checked exhaustiveness, `postpone`, three kinds of
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

An enter callback returns `Enter`, not `Next`, so postponing where there is
no event in hand is a compile error rather than a surprise at runtime.

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
