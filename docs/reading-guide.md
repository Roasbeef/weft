# Reading Weft's Gleam source

This guide assumes you already write programs with tagged unions, immutable
values and message passing. It connects those ideas to the Gleam patterns
used here, then gives a reading order through Weft's ownership and receive
loops. [Architecture](architecture.md) follows one run and records the
cross-module contracts; [the plan](plan.md) records design decisions and
work deliberately left open.

## Start with the types, then follow one event

Read [`weft/poll`](../src/weft/poll.gleam) first. It owns no process, so its
`Clock`, `Pass` and `Verdict` expose the state-threading pattern without
mailbox machinery. Follow `fold_until` into `loop`: `Pending(state)` passes
a replacement state to the next call, while `Settled` or `Broken` ends the
wait. The recursive call is the next iteration, not a spawned task.

Next read [`internal/timer`](../src/weft/internal/timer.gleam), especially
`Timers`, `Fired`, `Delivery`, `set` and `accept`. The book is an immutable
value. Every update returns the book the receive loop must retain. A call
that discards that result also discards the newest generation or cancellation
fact. The subject, in contrast, names a mailbox channel owned by a process.

Read [`weft/actor`](../src/weft/actor.gleam) from its `Flow` map, then `Next`,
`Initialised` and `Builder`. In the loop section, read `Self`, `Event` and
`Frozen` before `initialise_actor`. Follow a `Received` event through
`dispatch`, `handle` and `loop`; then follow a `Tick` through `handle_tick`.
[`internal/sys`](../src/weft/internal/sys.gleam) explains the separate system
request path and who sends each reply.

The state machine extends that reading with state-change policy. Read
[`Step`, `Target` and `TimeoutAction`](../src/weft/state_machine.gleam), then
`Self` and `commit`. A useful trace is a callback that postpones one request
while connecting, then transitions to an unequal state. `hold` stores the
request; `changed_state` reverses postponed events into arrival order and
`enter` can inject work ahead of the replay. The transition table beside
`Step` distinguishes moving to an equal state from an actual change.

Finally read the run engine's public `Outcome`, `PreparedTask`, `Ledger`
and `Run` in [`src/weft.gleam`](../src/weft.gleam). Follow `start` into `drive`
and `run_scope`, then read `Event`, `Proof`, `Consumer` and `Scope` before
`loop` and `step`. Use two traces: a plain worker returning `Ok(value)`, and
a managed worker returning while its owner's proof remains pending. They
meet at `note_outcome`; only the second waits in `Scope.awaiting`. Follow
`resolve_owner` and `apply_proof` to understand when it can leave.

The event manager and registry are smaller consumers of these patterns.
[`event_manager.handle`](../src/weft/event_manager.gleam) is a normal actor
callback; its heterogeneous state stays inside each handler closure.
[`internal/registry.handle`](../src/weft/internal/registry.gleam) serializes
bindings while ETS supplies direct reads. Read their outcome and binding
tables before their dispatches.

## Gleam patterns that carry the design

A `pub opaque type` exposes the type name while hiding its constructors.
Callers construct a `Run` with `weft.new` and modify it with setters rather
than writing its fields. Private types such as `Scope` and `Consumer` expose
all variants to this module's dispatch; `case` checks that each variant is
handled. A type alias, such as `Started`, reuses upstream OTP's type rather
than introducing a different runtime record.

`Next` and `Enter` are aliases of one `Step` with different phantom type
parameters. `Postponable` and `Unpostponable` have no constructors. Their
purpose is compile-time separation: the `postpone` function accepts the
former marker, so an enter callback cannot postpone a nonexistent event.
The marker does not add a runtime flag.

`Self(..self, state:, queue:)` constructs a new record by copying the fields
of `self` and replacing the named ones. The colon with no following value
is field punning: `state:` means `state: state`. `#(left, right)` is a tuple;
`#(state, data)` in a system reply preserves both parts of a machine's
state. Pattern matching binds fields, and `..` ignores fields not needed
by that branch.

A pipe passes its left value as the first argument to the next function.
`builder |> actor.on_message(handle) |> actor.start` is a sequence of calls,
not mutable builder updates. Named argument labels are optional at the call
site; they make intent visible without creating another calling convention.
A function capture such as `Next(_)` supplies one argument later.

`use value <- result.try(expression)` passes the remainder of the block as a
callback. If the expression is `Error`, `result.try` returns it without
running that continuation. `use handler <- list.filter_map(handlers)` is the
same syntax with a different contract: the remainder executes once per
handler and its `Ok` or `Error` determines whether that element remains.
Look up the qualified function after `<-` to understand what controls the
rest of the block.

A `Subject(message)` contains the owning process and a channel tag. Creating
it in the parent and receiving it in the child would target the wrong
mailbox. A `Selector(event)` combines typed channels, monitor messages and
system messages into one receive vocabulary. Mapping `Received`, `Fired` or
`System` explains which path a raw arrival takes into the loop. A selector
is not itself a process, a queue or a handler.

Links and monitors answer different ownership questions. A link propagates
exit signals in both directions; trapping converts ordinary signals into
`ExitMessage` values, while an untrappable kill still terminates the process.
A monitor observes death through one DOWN and does not transfer lifetime
ownership. Managed owners use monitors because killing the owner would
destroy the process whose exit is the drain evidence.

`@external` declares a foreign function with a Gleam signature. The compiler
checks callers against that signature, but the foreign implementation must
produce the promised representation. Read the reason beside the external
and its Erlang function together. Most foreign operations stay in the
internal boundaries; the engine's scheduler-count and terminal-exit BIFs
and poll's monotonic-clock BIF are existing documented exceptions.

## Principles for future source changes

Keep a short `//// ## Flow` in a large module. Name the actual entrypoint,
dispatch and domain helpers, and update the map when any name or path moves.
The map is a reading path, so it must distinguish alternatives from an
unconditional sequence. Do not introduce a helper merely to give the map
another function to name.

Place the state, message and action types before the implementation that
uses them. Put an entrypoint before its private callees where that improves
reading order, while keeping builder families and protocol sections together.
Small forward references between types are normal in Gleam and do not need
extra wrapper types or functions.

Keep domain calls qualified, such as `book.accept`, `sys.handle` and
`actor.continue`. Qualification distinguishes a timer-book operation from
a loop operation with a similar name. Give private helpers their actual
operation names, such as `resolve_owner`, `changed_state` or `retire`.
A name that repeats a signature's syntax hides more than it explains.

Put a compact transition table beside an important state or action type.
Use real constructors and events, name the function applying the transition,
and state any condition that makes the row legal. Keep independent facts
separate: `Consumer` governs delivery, `Proof` governs drain evidence, and
`Verdict` governs the scope's final exit. Combining them into invented phases
would obscure combinations the implementation actually supports.

Comments explain custody, ordering and failure behavior the syntax cannot
show. Give comments a blank line above them so a reader scanning the body
can find the next operation. Explain a monitor-before-permit ordering or a
reply-after-fan-out ordering at that operation. Avoid annotating ordinary
assignments with a paraphrase of the next line.

## Check a claim against code and tests

`make check` runs format, warning-free build, tests, lint and the doc graph
check. Capture its own exit code. `make docs` renders the public module and
function comments, making the reading order visible outside the raw source.
The doc graph checks mirrors and module headers; it does not prove that
flow maps or transition tables agree with the implementation.

For ownership, read `test/weft_managed_test.gleam` and `test/weft_test.gleam`.
For queue, replay, suspension and timeout rules, read the actor and
state-machine tests. `test/weft_internal_test.gleam` exercises timer and
system boundaries; registry and terminal-exit tests cover replacement
cleanup and exact failure reasons. A test demonstrates its scenario, not a
broader byte, CPU or wall-time bound that the implementation never enforces.
