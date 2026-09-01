//// Tests for `weft/state_machine`.
////
//// Almost every interesting property of a state machine is about *order* or
//// about *absence*: an event replayed before the mailbox rather than after
//// it, a timeout that must never be handled once its state is gone. Neither
//// kind is proved by a value, so the tests here work two ways.
////
//// Ordering tests are made deterministic rather than merely likely. The
//// mailbox is filled from inside the initialiser, before the loop has run at
//// all, which is the technique `weft_actor_test` uses for its continue
//// tests: every message the assertion depends on is already queued when the
//// machine takes its first step, so a wrong implementation cannot pass by
//// scheduling luck.
////
//// Absence tests carry a control. "The stale fire was flushed" is worth
//// nothing unless the same setup, with the transition removed, actually
//// delivers the fire — otherwise a timer that never fired at all would pass.
//// Each such pair is written as two tests that differ in one line.

import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/otp/system
import gleeunit
import weft/state_machine as sm

pub fn main() -> Nil {
  gleeunit.main()
}

// ------------------------------------------------------------- test tools

/// Stop a machine the test started, without taking the test process with it.
///
/// `start` links the machine to its caller, so killing it unlinked is the
/// only way for a test to clean up after itself.
fn discard(pid: Pid) -> Nil {
  process.unlink(pid)
  process.kill(pid)
}

/// The `#(state, data)` pair `sys:get_state/1` hands out, as a state name and
/// the log every machine in this file carries.
///
/// A state variant with no fields is an atom on the Erlang side, so the name
/// comes back as one; the pair is a tuple, which decoders index positionally.
fn view(pid: Pid) -> #(String, List(String)) {
  let raw = system.get_state(pid)
  let assert Ok(state) = decode.run(raw, decode.at([0], atom.decoder()))
    as "the machine's state is a nullary variant, so an atom"
  let assert Ok(log) =
    decode.run(raw, decode.at([1], decode.list(decode.string)))
    as "the machine's data is a list of strings"
  #(atom.to_string(state), log)
}

// ------------------------------------------------------- the probe machine

/// Three states, so that a test can tell a real transition from a
/// same-state one and still have somewhere else to go.
type Phase {
  Alpha
  Beta
  Gamma
}

fn phase_name(phase: Phase) -> String {
  case phase {
    Alpha -> "alpha"
    Beta -> "beta"
    Gamma -> "gamma"
  }
}

/// The events the ordering tests use. The data is a log of names, newest
/// first, and every arm that records anything records it there.
type Signal {
  /// Always handled where it lands, appending its name to the log.
  Note(name: String)

  /// Postponed in the states a test names, handled anywhere else. This is
  /// the event whose replay order the assertions are about.
  Wait(name: String)

  /// Move to a phase, logging `go` and injecting a `Note` so that every
  /// transition leaves a visible mark on both sides of the enter callback.
  Go(to: Phase)

  /// Stop normally.
  Halt

  /// Report the log, oldest first.
  Report(reply: Subject(List(String)))
}

/// An event handler that postpones `Wait` in the phases given and handles it
/// everywhere else.
///
/// The list is a parameter rather than three near-identical handlers because
/// which states hold an event is exactly the variable these tests vary.
fn holding_in(
  phases: List(Phase),
) -> fn(Phase, List(String), Signal) -> sm.Next(Phase, List(String), Signal) {
  fn(phase: Phase, log: List(String), message: Signal) {
    case message {
      Wait(name:) ->
        case list.contains(phases, phase) {
          True -> sm.keep(log) |> sm.postpone
          False -> sm.keep([name, ..log])
        }

      Note(name:) -> sm.keep([name, ..log])

      Go(to:) ->
        sm.transition(to:, data: ["go", ..log])
        |> sm.then_handle(Note("injected"))

      Halt -> sm.stop()

      Report(reply:) -> {
        process.send(reply, list.reverse(log))
        sm.keep(log)
      }
    }
  }
}

/// An enter callback that records the transition it was called for.
///
/// The initial call is the one where `from` and `to` are equal, which cannot
/// happen otherwise: a same-state transition is not a state change and runs
/// no enter call at all.
fn tracing_enter(
  from: Phase,
  to: Phase,
  log: List(String),
) -> sm.Enter(Phase, List(String), Signal) {
  sm.keep([phase_name(from) <> "->" <> phase_name(to), ..log])
}

// ------------------------------------------------------- postpone + replay

pub fn postponed_events_replay_in_arrival_order_exactly_once_test() -> Nil {
  let assert Ok(started) =
    sm.new_with_initialiser(1000, fn(subject) {
      // Every message the assertion depends on is in the mailbox before the
      // loop takes its first step, so the ordering below cannot be reached
      // by luck.
      process.send(subject, Wait("a"))
      process.send(subject, Wait("b"))
      process.send(subject, Wait("c"))
      process.send(subject, Go(Beta))
      process.send(subject, Note("mailbox"))
      sm.initialised(Alpha, []) |> sm.returning(subject) |> Ok
    })
    |> sm.on_event(holding_in([Alpha]))
    |> sm.start
    as "the machine must start"

  // `injected` is the block the transition itself queued, so it is ahead of
  // the replay; the three waits come back in the order they arrived, once
  // each; the mailbox is last.
  assert sm.call(started.data, waiting: 1000, sending: Report)
    == ["go", "injected", "a", "b", "c", "mailbox"]

  discard(started.pid)
}

pub fn an_event_may_be_postponed_again_in_the_new_state_test() -> Nil {
  let assert Ok(started) =
    sm.new_with_initialiser(1000, fn(subject) {
      process.send(subject, Wait("a"))
      process.send(subject, Wait("b"))
      process.send(subject, Wait("c"))
      process.send(subject, Go(Beta))
      process.send(subject, Go(Gamma))
      process.send(subject, Note("mailbox"))
      sm.initialised(Alpha, []) |> sm.returning(subject) |> Ok
    })
    |> sm.on_event(holding_in([Alpha, Beta]))
    |> sm.start
    as "the machine must start"

  // Held in Alpha, replayed into Beta and held again there, replayed into
  // Gamma and finally handled: still once each, still in arrival order.
  assert sm.call(started.data, waiting: 1000, sending: Report)
    == ["go", "injected", "go", "injected", "a", "b", "c", "mailbox"]

  discard(started.pid)
}

pub fn enter_then_injected_then_replayed_then_mailbox_test() -> Nil {
  // The ordering everyone gets wrong, in one assertion. Four sources of work
  // are pending at the same instant and each must land in its own place.
  let assert Ok(started) =
    sm.new_with_initialiser(1000, fn(subject) {
      process.send(subject, Wait("p1"))
      process.send(subject, Wait("p2"))
      process.send(subject, Go(Beta))
      process.send(subject, Note("mailbox"))
      sm.initialised(Alpha, []) |> sm.returning(subject) |> Ok
    })
    |> sm.on_event(holding_in([Alpha]))
    |> sm.on_enter(entering_note)
    |> sm.start
    as "the machine must start"

  // `go` is the handler's own data change, then the enter callback runs and
  // its injected block goes in front of the handler's, then the replay, then
  // the mailbox.
  assert sm.call(started.data, waiting: 1000, sending: Report)
    == ["go", "enter", "injected", "p1", "p2", "mailbox"]

  discard(started.pid)
}

/// An enter callback that injects a message rather than logging one, so that
/// the ordering test can see where the enter block lands relative to the
/// handler's.
fn entering_note(
  from: Phase,
  to: Phase,
  log: List(String),
) -> sm.Enter(Phase, List(String), Signal) {
  case from == to {
    // The initial call, which this test wants to be silent.
    True -> sm.keep(log)
    False -> sm.keep(log) |> sm.then_handle(Note("enter"))
  }
}

// ------------------------------------------------------- enter callbacks

pub fn enter_runs_for_the_initial_state_and_each_real_transition_test() -> Nil {
  let assert Ok(started) =
    sm.new(Alpha, [])
    |> sm.on_event(holding_in([]))
    |> sm.on_enter(tracing_enter)
    |> sm.start
    as "the machine must start"

  sm.send(started.data, Go(Beta))
  // A transition to the state the machine is already in is not a state
  // change, so this one leaves no `beta->beta` behind.
  sm.send(started.data, Go(Beta))
  sm.send(started.data, Go(Gamma))

  assert sm.call(started.data, waiting: 1000, sending: Report)
    == [
      "alpha->alpha", "go", "alpha->beta", "injected", "go", "injected", "go",
      "beta->gamma", "injected",
    ]

  discard(started.pid)
}

pub fn an_enter_callback_may_transition_again_test() -> Nil {
  let assert Ok(started) =
    sm.new(Alpha, [])
    |> sm.on_event(holding_in([]))
    |> sm.on_enter(passing_through_beta)
    |> sm.start
    as "the machine must start"

  sm.send(started.data, Go(Beta))

  // Beta is a pass-through state: entering it moves straight on to Gamma,
  // and the second enter call runs before any queued work.
  assert sm.call(started.data, waiting: 1000, sending: Report)
    == ["alpha->alpha", "go", "alpha->beta", "beta->gamma", "injected"]

  let #(state, _log) = view(started.pid)
  assert state == "gamma"

  discard(started.pid)
}

/// An enter callback that chains: entering Beta transitions on to Gamma.
fn passing_through_beta(
  from: Phase,
  to: Phase,
  log: List(String),
) -> sm.Enter(Phase, List(String), Signal) {
  let log = [phase_name(from) <> "->" <> phase_name(to), ..log]
  case to {
    Beta -> sm.transition(to: Gamma, data: log)
    Alpha | Gamma -> sm.keep(log)
  }
}
