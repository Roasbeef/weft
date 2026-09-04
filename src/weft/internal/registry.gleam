//// The registry owner and its minimal ETS boundary.
////
//// Binding, conflict detection and monitor cleanup are Gleam actor handlers.
//// The upstream actor suffices here and keeps the dependency graph acyclic:
//// weft's richer builders depend on this registry for addressed startup.
//// Erlang supplies only unnamed ETS operations, local-subject validation,
//// without implementing a receive loop or server callbacks.
////
//// Only the public registry mints keys. Its opaque Address ties each key to
//// one message type, so the table can erase subjects without allowing a
//// caller to retrieve one under another type. A stopped owner destroys its
//// table; lookups then return unavailable rather than aliasing a new owner.

import gleam/bool
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Monitor, type Pid, type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/otp/actor
import gleam/result

/// The owner inbox and its unnamed table; neither is a permanent name.
pub opaque type Registry {
  Registry(inbox: Subject(Message), table: Table, owner: Pid)
}

type Table

type Binding {
  Binding(recipient: Pid, subject: Dynamic, monitor: Monitor)
}

type Message {
  Bind(
    key: Reference,
    recipient: Pid,
    subject: Dynamic,
    reply: Subject(Result(Nil, String)),
  )
  Departed(process.Down)
  Stop
}

type State {
  State(table: Table, monitors: Dict(Monitor, Reference))
}

/// Starts the linked OTP owner.
///
/// ## Examples
///
/// ```gleam
/// let started = registry.start()
/// ```
pub fn start() -> Result(Registry, String) {
  actor.new_with_initialiser(1000, fn(inbox) {
    let table = new_table()
    let selector =
      process.new_selector()
      |> process.select(inbox)
      |> process.select_monitors(Departed)
    actor.initialised(State(table:, monitors: dict.new()))
    |> actor.selecting(selector)
    |> actor.returning(Registry(inbox:, table:, owner: process.self()))
    |> Ok
  })
  |> actor.on_message(handle)
  |> actor.start
  |> result.map(fn(started) { started.data })
  |> result.replace_error("registry failed to start")
}

/// Serializes binding changes and installs the recipient monitor.
///
/// ## Examples
///
/// ```gleam
/// let bound = registry.register(names, key, inbox)
/// ```
pub fn register(
  registry: Registry,
  key: Reference,
  subject: Subject(message),
) -> Result(Nil, String) {
  register_within(registry, key, subject, 5000)
}

/// Registers with an explicit deadline for deterministic timeout tests.
///
/// ## Examples
///
/// ```gleam
/// let bound = registry.register_within(names, key, inbox, 1000)
/// ```
pub fn register_within(
  registry: Registry,
  key: Reference,
  subject: Subject(message),
  within: Int,
) -> Result(Nil, String) {
  use recipient <- result.try(local_owner(subject))
  let reply = process.new_subject()
  let monitor = process.monitor(registry.owner)
  process.send(
    registry.inbox,
    Bind(key, recipient, erase_subject(subject), reply),
  )
  let answer =
    process.new_selector()
    |> process.select_map(reply, Ok)
    |> process.select_specific_monitor(monitor, fn(_) { Error(Nil) })
    |> process.selector_receive(within)

  // A timeout returns to a live caller. Its monitor must be released on
  // every outcome, not left behind by a caught upstream call panic.
  process.demonitor_process(monitor)
  answer
  |> result.flatten
  |> result.replace_error("registry unavailable")
  |> result.flatten
}

fn handle(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    Bind(key, recipient, subject, reply) -> {
      let #(next, answer) = bind(state, key, recipient, subject)
      process.send(reply, answer)
      actor.continue(next)
    }
    Departed(process.ProcessDown(monitor:, ..)) ->
      actor.continue(retire(state, monitor))
    Departed(process.PortDown(..)) -> actor.continue(state)
    Stop -> actor.stop()
  }
}

fn bind(
  state: State,
  key: Reference,
  recipient: Pid,
  subject: Dynamic,
) -> #(State, Result(Nil, String)) {
  use <- bool.guard(when: !process.is_alive(recipient), return: #(
    state,
    Error("recipient is not alive"),
  ))
  case read(state.table, key) {
    Ok(binding) if binding.subject == subject -> #(state, Ok(Nil))
    Ok(binding) -> {
      use <- bool.guard(when: process.is_alive(binding.recipient), return: #(
        state,
        Error("address already registered"),
      ))

      // Replacement does not wait for DOWN delivery. Remove that monitor's
      // custody before publishing a fresh one; replay cannot erase the new row.
      process.demonitor_process(binding.monitor)
      publish(
        State(..state, monitors: dict.delete(state.monitors, binding.monitor)),
        key,
        recipient,
        subject,
      )
    }
    Error(Nil) -> publish(state, key, recipient, subject)
  }
}

fn publish(
  state: State,
  key: Reference,
  recipient: Pid,
  subject: Dynamic,
) -> #(State, Result(Nil, String)) {
  // Monitor first. Even an immediate death is handled only after this
  // callback publishes both the binding and its cleanup responsibility.
  let monitor = process.monitor(recipient)
  put(state.table, key, Binding(recipient:, subject:, monitor:))
  #(
    State(..state, monitors: dict.insert(state.monitors, monitor, key)),
    Ok(Nil),
  )
}

fn retire(state: State, monitor: Monitor) -> State {
  case dict.get(state.monitors, monitor) {
    Error(Nil) -> state
    Ok(key) -> {
      case read(state.table, key) {
        Ok(binding) if binding.monitor == monitor -> delete(state.table, key)
        Ok(_) | Error(Nil) -> Nil
      }
      State(..state, monitors: dict.delete(state.monitors, monitor))
    }
  }
}

/// Reads a live binding directly from ETS.
///
/// ## Examples
///
/// ```gleam
/// let current = registry.lookup(names, key)
/// ```
pub fn lookup(
  registry: Registry,
  key: Reference,
) -> Result(Subject(message), Nil) {
  lookup_subject(registry.table, key)
}

/// Returns the registry process, not any recipient.
///
/// ## Examples
///
/// ```gleam
/// let monitor = process.monitor(registry.owner(names))
/// ```
pub fn owner(registry: Registry) -> Pid {
  registry.owner
}

/// Requests registry shutdown and waits for its owner to exit.
///
/// ## Examples
///
/// ```gleam
/// let stopped = registry.stop(names)
/// ```
pub fn stop(registry: Registry) -> Result(Nil, String) {
  let monitor = process.monitor(owner(registry))
  process.send(registry.inbox, Stop)
  let stopped =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(5000)
  process.demonitor_process(monitor)
  stopped |> result.replace(Nil) |> result.replace_error("registry unavailable")
}

@external(erlang, "weft_registry_ffi", "new_table")
fn new_table() -> Table

@external(erlang, "weft_registry_ffi", "read")
fn read(table: Table, key: Reference) -> Result(Binding, Nil)

@external(erlang, "weft_registry_ffi", "put")
fn put(table: Table, key: Reference, binding: Binding) -> Nil

@external(erlang, "weft_registry_ffi", "delete")
fn delete(table: Table, key: Reference) -> Nil

@external(erlang, "weft_registry_ffi", "lookup")
fn lookup_subject(table: Table, key: Reference) -> Result(Subject(message), Nil)

@external(erlang, "weft_registry_ffi", "local_owner")
fn local_owner(subject: Subject(message)) -> Result(Pid, String)

// Keys are minted only behind Address(message), preserving the erased type
// relation for lookup without permitting arbitrary dynamic data at this edge.
@external(erlang, "gleam_stdlib", "identity")
fn erase_subject(subject: Subject(message)) -> Dynamic
