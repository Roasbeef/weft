# weft

Owned, bounded structured concurrency for Gleam, plus typed bindings for the
OTP behaviours the official bindings don't cover.

The name comes from weaving. The warp threads are the ones held under tension
on the loom, the long lived structure, which is what a supervision tree is.
The weft is the thread drawn across them, one transient pass at a time, which
is what a task is.

## What's in the box

**`weft`** is the run engine: fan a list of tasks out across a bounded pool
of processes and get back a complete account of what happened to every one
of them, including the ones that were cancelled mid flight and the ones that
never got a slot. Every task is owned by a scope process, and the link
topology enforces the guarantee the API states: no task outlives its scope,
and no scope outlives its caller. See
[#1](https://github.com/Roasbeef/weft/issues/1) for the full design.

```gleam
let outcomes =
  urls
  |> list.map(fn(url) { fn() { fetch_and_parse(url) } })
  |> weft.new
  |> weft.limit(8)
  |> weft.on_failure(weft.KeepGoing)
  |> weft.deadline(30_000)
  |> weft.start
```

**`weft/state_machine`** is a typed `gen_statem`: state ADTs with
compiler-checked exhaustiveness, `postpone`, state and event timeouts, and
enter callbacks. The two features that delete the most hand-rolled code are
postponing an event until the next state change and a timeout that is
cancelled by leaving the state. See
[#2](https://github.com/Roasbeef/weft/issues/2).

**`weft/event_manager`** is a typed `gen_event`: a manager process fanning
events out to handlers that each carry their own private state, encoded as
successor closures so the handler list stays homogeneous. A handler that
fails is dropped and logged without disturbing its siblings, which is the
whole value of the pattern. See
[#3](https://github.com/Roasbeef/weft/issues/3).

**`weft/actor`** is a strict superset of `gleam/otp/actor`'s builder with
the gen_server features the upstream module doesn't carry: a
`handle_continue` analog (`continuing` / `then_handle`), an `on_shutdown`
cleanup callback, hibernation, and an idle timeout. See
[#4](https://github.com/Roasbeef/weft/issues/4).

## Status

Pre-release. The designs live in the issues linked above; the API surface
there is the plan of record until a 1.0 freezes it.

## Relationship to gleam_otp

Weft is not a fork and not a competing framework. `gleam_otp`'s actor and
supervisors are the right tools for long lived services, and weft's types
interoperate with them directly: a `weft/state_machine` or
`weft/event_manager` hands back a `supervision.ChildSpecification` and slots
into a static supervisor like anything else. Weft covers the ground upstream
has (so far) chosen not to: transient task fan-out with an ownership
guarantee, and the behaviours whose Erlang APIs don't survive typing without
a redesign.

## Development

`make help` lists the common commands. `make check` is the full gate:
format check, warning-free build, tests, the house lint, and the doc graph
check. The linter under `tools/lint` is borrowed from
[loom](https://github.com/Roasbeef/loom) and vendored for now.

## License

Apache-2.0
