//// Tests for `weft/actor`.
////
//// The interesting ones are about *order*, not about values: a continue
//// beating the mailbox, a `then_handle` block beating what was queued
//// before it, a `suspend` landing between two continues. Order tests are
//// only worth anything if they can actually lose, so the race tests drive
//// the parent as hard as they can and repeat, and the timing tests give the
//// actor real quiet periods rather than mocking a clock.

import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/otp/actor as otp_actor
import gleam/otp/static_supervisor as supervisor
import gleam/otp/system
import gleam/string
import gleeunit
import weft/actor

pub fn main() -> Nil {
  gleeunit.main()
}

// ------------------------------------------------------------- test tools

/// Stop an actor the test started, without taking the test process with it.
///
/// `start` links the actor to its caller, so killing it unlinked is the
/// only way for a test to clean up after itself.
fn discard(pid: Pid) -> Nil {
  process.unlink(pid)
  process.kill(pid)
}

/// `sys:get_status/1` has no Gleam binding. The observer calls it, so a test
/// that wants to prove the observer would render this actor has to call it
/// the same way.
@external(erlang, "sys", "get_status")
fn get_status(pid: Pid) -> Dynamic

/// Read an atom out of a nested position in a raw Erlang term.
fn atom_at(data: Dynamic, path: List(Int)) -> String {
  let assert Ok(value) = decode.run(data, decode.at(path, atom.decoder()))
    as "the status reply must have an atom at this position"
  atom.to_string(value)
}

// ------------------------------------------------- ordering: the continue

/// A message that records the order it was handled in.
type Probe {
  /// Handled from the continue queue.
  Injected(tag: String)
  /// Handled from the mailbox.
  External(tag: String)
  /// Reports the log so far, oldest first.
  Report(reply: Subject(List(String)))
}

fn probe_handler(
  log: List(String),
  message: Probe,
) -> actor.Next(List(String), Probe) {
  case message {
    Injected(tag:) -> actor.continue([tag, ..log])
    External(tag:) -> actor.continue([tag, ..log])
    Report(reply:) -> {
      process.send(reply, list.reverse(log))
      actor.continue(log)
    }
  }
}

pub fn a_continue_is_handled_before_the_mailbox_test() -> Nil {
  // Repeated, because this is a scheduling race: one green run proves
  // nothing about an interleaving that happens one time in fifty.
  use _attempt <- list.each(list.repeat(Nil, 100))

  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      actor.initialised([])
      |> actor.returning(subject)
      |> actor.continuing(Injected("continue"))
      |> Ok
    })
    |> actor.on_message(probe_handler)
    |> actor.start
    as "the actor must start"

  // `start` has returned, so the acknowledgement is already sent and the
  // actor may not have reached its receive loop yet. This send is the one
  // that would win under a self-send-from-the-initialiser workaround.
  process.send(started.data, External("mailbox"))

  assert actor.call(started.data, waiting: 1000, sending: Report)
    == ["continue", "mailbox"]

  discard(started.pid)
}

pub fn a_continue_beats_a_mailbox_that_is_already_full_test() -> Nil {
  // The racing test above can only lose by a scheduling accident. This one
  // cannot lose by accident at all: the mailbox is *guaranteed* non-empty
  // when the loop starts, because the initialiser filled it itself. A
  // continue implemented as a self-send would be behind these two; a
  // continue implemented as a queue the loop drains first is ahead of them.
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      process.send(subject, External("mailbox"))
      process.send(subject, External("mailbox"))
      actor.initialised([])
      |> actor.returning(subject)
      |> actor.continuing(Injected("continue"))
      |> Ok
    })
    |> actor.on_message(probe_handler)
    |> actor.start
    as "the actor must start"

  assert actor.call(started.data, waiting: 1000, sending: Report)
    == ["continue", "mailbox", "mailbox"]

  discard(started.pid)
}

pub fn several_continues_run_in_the_order_given_test() -> Nil {
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      actor.initialised([])
      |> actor.returning(subject)
      |> actor.continuing(Injected("first"))
      |> actor.continuing(Injected("second"))
      |> Ok
    })
    |> actor.on_message(probe_handler)
    |> actor.start
    as "the actor must start"

  process.send(started.data, External("mailbox"))

  assert actor.call(started.data, waiting: 1000, sending: Report)
    == ["first", "second", "mailbox"]

  discard(started.pid)
}

/// A handler that injects a block of its own, so that the queue has both an
/// older entry and a newer block in it at once.
fn chain_handler(
  log: List(String),
  message: Probe,
) -> actor.Next(List(String), Probe) {
  case message {
    Injected(tag: "a") ->
      actor.continue(["a", ..log])
      |> actor.then_handle(Injected("b"))
      |> actor.then_handle(Injected("c"))
    Injected(tag:) -> actor.continue([tag, ..log])
    External(tag:) -> actor.continue([tag, ..log])
    Report(reply:) -> {
      process.send(reply, list.reverse(log))
      actor.continue(log)
    }
  }
}

pub fn then_handle_is_depth_first_test() -> Nil {
  // The queue starts as [a, d]. Handling `a` injects [b, c], which must go
  // in *front* of `d` and in the order written: a, b, c, d, and only then
  // whatever the mailbox holds.
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      actor.initialised([])
      |> actor.returning(subject)
      |> actor.continuing(Injected("a"))
      |> actor.continuing(Injected("d"))
      |> Ok
    })
    |> actor.on_message(chain_handler)
    |> actor.start
    as "the actor must start"

  process.send(started.data, External("mailbox"))

  assert actor.call(started.data, waiting: 1000, sending: Report)
    == ["a", "b", "c", "d", "mailbox"]

  discard(started.pid)
}

// --------------------------------------------------- the debug plane

/// A message that walks a long continue chain, one step at a time, slowly
/// enough that a test can interrupt it.
type Walk {
  Step(remaining: Int)
}

fn walk_handler(done: Int, message: Walk) -> actor.Next(Int, Walk) {
  case message {
    Step(remaining: 0) -> actor.continue(done + 1)
    Step(remaining:) -> {
      // Deliberately slow: the chain has to still be running when the test
      // suspends it, or the test proves nothing.
      process.sleep(2)
      actor.continue(done + 1) |> actor.then_handle(Step(remaining - 1))
    }
  }
}

fn state_of(pid: Pid) -> Int {
  let assert Ok(count) = decode.run(system.get_state(pid), decode.int)
    as "this actor's state is an Int"
  count
}

pub fn suspend_halts_a_continue_chain_and_resume_finishes_it_test() -> Nil {
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      actor.initialised(0)
      |> actor.returning(subject)
      |> actor.continuing(Step(60))
      |> Ok
    })
    |> actor.on_message(walk_handler)
    |> actor.start
    as "the actor must start"

  // Land the suspend in the middle of the chain. `sys:suspend/1` blocks
  // until the actor acknowledges, so this returning at all is the proof
  // that the debug plane is served between continues rather than after
  // them.
  process.sleep(20)
  system.suspend(started.pid)

  let frozen = state_of(started.pid)
  assert frozen < 61
  process.sleep(60)

  // Still frozen: a suspended actor may not drain its own queue either.
  assert state_of(started.pid) == frozen

  system.resume(started.pid)
  process.sleep(400)
  assert state_of(started.pid) == 61

  discard(started.pid)
}

// ------------------------------------------------- a general-purpose actor

/// One message type shared by the tests that need a plain counter.
type Counter {
  /// Add one.
  Bump
  /// Do nothing, but count as traffic.
  Poke
  /// Stop normally.
  Halt
  /// Report the count.
  Count(reply: Subject(Int))
}

fn counter_handler(count: Int, message: Counter) -> actor.Next(Int, Counter) {
  case message {
    Bump -> actor.continue(count + 1)
    Poke -> actor.continue(count)
    Halt -> actor.stop()
    Count(reply:) -> {
      process.send(reply, count)
      actor.continue(count)
    }
  }
}

pub fn get_state_reports_the_current_state_test() -> Nil {
  let assert Ok(started) =
    actor.new(0) |> actor.on_message(counter_handler) |> actor.start
    as "the actor must start"

  process.send(started.data, Bump)
  process.send(started.data, Bump)
  process.send(started.data, Bump)

  // Sync on a call so that the three bumps are certainly handled before the
  // state is read out of band.
  assert actor.call(started.data, waiting: 1000, sending: Count) == 3
  assert state_of(started.pid) == 3

  discard(started.pid)
}

pub fn get_status_replies_in_the_shape_the_observer_reads_test() -> Nil {
  let assert Ok(started) =
    actor.new(0) |> actor.on_message(counter_handler) |> actor.start
    as "the actor must start"

  let status = get_status(started.pid)

  // `{status, Pid, {module, Module}, [PDict, Mode, Parent, Debug, Format]}`
  // is what `sys:get_status/1` specifies and what the observer's process
  // view renders.
  assert atom_at(status, [0]) == "status"
  assert atom_at(status, [2, 0]) == "module"
  assert atom_at(status, [2, 1]) == "weft@actor"
  assert atom_at(status, [3, 1]) == "running"

  let assert Ok(parent) = decode.run(status, decode.at([3, 2], decode.dynamic))
    as "the status report names the parent"
  assert string.inspect(parent) == string.inspect(process.self())

  let assert Ok(sections) =
    decode.run(status, decode.at([3, 4], decode.list(decode.dynamic)))
    as "the status report carries the observer's format sections"
  assert list.length(sections) == 3

  discard(started.pid)
}

pub fn a_suspended_actor_still_answers_get_state_test() -> Nil {
  let assert Ok(started) =
    actor.new(0) |> actor.on_message(counter_handler) |> actor.start
    as "the actor must start"

  process.send(started.data, Bump)
  assert actor.call(started.data, waiting: 1000, sending: Count) == 1

  system.suspend(started.pid)

  // The bump is now stuck in the mailbox behind the suspension, so the
  // state read out of band must still be the old one.
  process.send(started.data, Bump)
  process.sleep(50)
  assert state_of(started.pid) == 1

  system.resume(started.pid)
  assert actor.call(started.data, waiting: 1000, sending: Count) == 2

  discard(started.pid)
}

// ---------------------------------------------------------- upstream parity

/// The stack from `gleam/otp/actor`'s module documentation, unchanged.
type Stack(element) {
  Shutdown
  Push(push: element)
  Pop(reply_with: Subject(Result(element, Nil)))
}

fn handle_stack(
  stack: List(e),
  message: Stack(e),
) -> actor.Next(List(e), Stack(e)) {
  case message {
    Shutdown -> actor.stop()

    Push(value) -> {
      let new_state = [value, ..stack]
      actor.continue(new_state)
    }

    Pop(client) ->
      case stack {
        [] -> {
          process.send(client, Error(Nil))
          actor.continue([])
        }

        [first, ..rest] -> {
          process.send(client, Ok(first))
          actor.continue(rest)
        }
      }
  }
}

pub fn the_upstream_readme_example_works_verbatim_test() -> Nil {
  let assert Ok(started) =
    actor.new([]) |> actor.on_message(handle_stack) |> actor.start
    as "the actor must start"
  let subject = started.data

  process.send(subject, Push("Joe"))
  process.send(subject, Push("Mike"))
  process.send(subject, Push("Robert"))

  let assert Ok("Robert") = process.call(subject, 10, Pop)
    as "the stack pops in reverse order"
  let assert Ok("Mike") = process.call(subject, 10, Pop)
    as "the stack pops in reverse order"
  let assert Ok("Joe") = process.call(subject, 10, Pop)
    as "the stack pops in reverse order"
  let assert Error(Nil) = process.call(subject, 10, Pop)
    as "an empty stack replies with an error"

  process.send(subject, Shutdown)
  process.sleep(50)
  assert !process.is_alive(started.pid)
}

pub fn a_named_actor_is_reachable_by_its_name_test() -> Nil {
  let name = process.new_name("weft_actor_named")

  let assert Ok(started) =
    actor.new(0)
    |> actor.named(name)
    |> actor.on_message(counter_handler)
    |> actor.start
    as "the actor must start"

  let subject = process.named_subject(name)
  process.send(subject, Bump)
  assert actor.call(subject, waiting: 1000, sending: Count) == 1

  discard(started.pid)
}

pub fn a_weft_actor_runs_under_a_gleam_otp_supervisor_test() -> Nil {
  let name = process.new_name("weft_actor_supervised")

  let child =
    actor.new(0)
    |> actor.named(name)
    |> actor.on_message(counter_handler)
    |> actor.supervised

  let assert Ok(started) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(child)
    |> supervisor.start
    as "the supervisor must start its weft child"

  let subject = process.named_subject(name)
  process.send(subject, Bump)
  assert actor.call(subject, waiting: 1000, sending: Count) == 1

  discard(started.pid)
}

pub fn a_custom_selector_composes_with_a_continue_test() -> Nil {
  // `selecting` fixes the message type here where upstream's changes it, so
  // that a queue filled by `continuing` survives it. This is the two
  // together, which is the combination that signature exists to allow.
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      // A second subject the *actor* owns, so that messages sent to it land
      // in the actor's mailbox rather than the test's.
      let side = process.new_subject()
      let selector =
        process.new_selector()
        |> process.select(subject)
        |> process.select_map(side, External)

      actor.initialised([])
      |> actor.selecting(selector)
      |> actor.returning(#(subject, side))
      |> actor.continuing(Injected("continue"))
      |> Ok
    })
    |> actor.on_message(probe_handler)
    |> actor.start
    as "the actor must start"

  let #(own, side) = started.data
  process.send(side, "side")
  process.send(own, External("own"))

  assert actor.call(own, waiting: 1000, sending: Report)
    == ["continue", "side", "own"]

  discard(started.pid)
}

/// State for the `with_selector` test: the actor has to hold on to both
/// subjects, because the selector it installs mid-flight has to name them.
type Widen {
  Widen(log: List(String), own: Subject(Probe), side: Subject(String))
}

fn widen_handler(state: Widen, message: Probe) -> actor.Next(Widen, Probe) {
  case message {
    // Widening from inside a handler: until this runs, the side subject is
    // not selected for and anything sent to it is discarded as unexpected.
    Injected(tag: "widen") -> {
      let selector =
        process.new_selector()
        |> process.select(state.own)
        |> process.select_map(state.side, External)

      actor.continue(Widen(..state, log: ["widen", ..state.log]))
      |> actor.with_selector(selector)
    }

    Injected(tag:) -> actor.continue(Widen(..state, log: [tag, ..state.log]))
    External(tag:) -> actor.continue(Widen(..state, log: [tag, ..state.log]))
    Report(reply:) -> {
      process.send(reply, list.reverse(state.log))
      actor.continue(state)
    }
  }
}

pub fn with_selector_widens_what_the_actor_receives_test() -> Nil {
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      let side = process.new_subject()
      actor.initialised(Widen(log: [], own: subject, side:))
      |> actor.returning(#(subject, side))
      |> actor.continuing(Injected("widen"))
      |> Ok
    })
    |> actor.on_message(widen_handler)
    |> actor.start
    as "the actor must start"

  let #(own, side) = started.data
  process.send(side, "side")
  process.send(own, External("own"))

  assert actor.call(own, waiting: 1000, sending: Report)
    == ["widen", "side", "own"]

  discard(started.pid)
}

pub fn an_initialiser_error_is_reported_as_init_failed_test() -> Nil {
  let result =
    actor.new_with_initialiser(1000, fn(_subject) { Error("no disk") })
    |> actor.on_message(counter_handler)
    |> actor.start

  assert result == Error(otp_actor.InitFailed("no disk"))
}
