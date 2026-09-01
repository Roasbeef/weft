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

// ------------------------------------------------------ the clockwork machine

/// The events the timeout tests use. Every arm is a way to arm, cancel or
/// observe a timer, so that each cancellation rule can be exercised on its
/// own.
type Clockwork {
  /// Arm a state timeout that rings `state`.
  StateTimeoutIn(ms: Int)

  /// Arm a state timeout of zero milliseconds and queue a `Doze` behind it,
  /// which is how a test forces a fire to be genuinely in flight when it is
  /// cancelled.
  Trip(to: Phase)

  /// Sleep long enough for a zero-millisecond timer to have landed in the
  /// mailbox, then transition. Handled from the injected queue, so the
  /// transition happens before the loop looks at the mailbox at all.
  Doze(to: Phase)

  /// Arm an event timeout that rings `event`.
  EventTimeoutIn(ms: Int)

  /// Arm a named timeout that rings its own name.
  NamedTimeoutIn(name: String, ms: Int)

  /// Cancel a named timeout.
  CancelNamed(name: String)

  /// A timeout fired. Recorded in the log.
  Rang(name: String)

  /// Transition to the state the machine is already in.
  Stay

  /// Transition to a different state.
  Move(to: Phase)

  /// Transition to a different state and arm a state timeout in the same
  /// step, which is the ordering corner: the cancellation belongs to the
  /// state being left, not to the step doing the leaving.
  MoveArming(to: Phase, ms: Int)

  /// An event that changes nothing, and exists only to be traffic.
  Quiet

  /// Report which timeouts have rung, oldest first.
  Rings(reply: Subject(List(String)))
}

fn clockwork_handler(
  phase: Phase,
  log: List(String),
  message: Clockwork,
) -> sm.Next(Phase, List(String), Clockwork) {
  case message {
    StateTimeoutIn(ms:) ->
      sm.keep(log) |> sm.with_state_timeout(after: ms, sending: Rang("state"))

    // The arm and the move are one chain, so the move is handled from the
    // injected queue — ahead of the mailbox the fire is sitting in. The
    // `Doze` in between is what makes "sitting in" true rather than hopeful:
    // without it the cancel usually wins the race and the flush is never
    // exercised at all.
    Trip(to:) ->
      sm.keep(log)
      |> sm.with_state_timeout(after: 0, sending: Rang("state"))
      |> sm.then_handle(Doze(to:))

    Doze(to:) -> {
      process.sleep(30)
      sm.transition(to:, data: log)
    }

    EventTimeoutIn(ms:) ->
      sm.keep(log) |> sm.with_event_timeout(after: ms, sending: Rang("event"))

    NamedTimeoutIn(name:, ms:) ->
      sm.keep(log)
      |> sm.with_named_timeout(name:, after: ms, sending: Rang(name))

    CancelNamed(name:) -> sm.keep(log) |> sm.cancel_timeout(name:)

    Rang(name:) -> sm.keep([name, ..log])

    Stay -> sm.transition(to: phase, data: log)

    Move(to:) -> sm.transition(to:, data: log)

    MoveArming(to:, ms:) ->
      sm.transition(to:, data: log)
      |> sm.with_state_timeout(after: ms, sending: Rang("state"))

    Quiet -> sm.keep(log)

    Rings(reply:) -> {
      process.send(reply, list.reverse(log))
      sm.keep(log)
    }
  }
}

fn start_clockwork() -> sm.Started(Subject(Clockwork)) {
  let assert Ok(started) =
    sm.new(Alpha, []) |> sm.on_event(clockwork_handler) |> sm.start
    as "the machine must start"
  started
}

// ---------------------------------------------------------- state timeouts

pub fn a_state_timeout_fires_while_the_state_is_kept_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, StateTimeoutIn(40))
  process.sleep(150)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["state"]

  discard(started.pid)
}

pub fn keep_does_not_cancel_a_state_timeout_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, StateTimeoutIn(60))
  sm.send(started.data, Quiet)
  process.sleep(200)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["state"]

  discard(started.pid)
}

pub fn a_same_state_transition_does_not_cancel_a_state_timeout_test() -> Nil {
  // gen_statem's awkward corner, kept rather than smoothed over: moving to
  // the state you are in is not a state change.
  let started = start_clockwork()

  sm.send(started.data, StateTimeoutIn(60))
  sm.send(started.data, Stay)
  process.sleep(200)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["state"]

  discard(started.pid)
}

pub fn a_real_transition_cancels_a_state_timeout_test() -> Nil {
  // The same test as above with one line changed, which is what makes the
  // pair worth anything: the only difference is where the transition goes.
  let started = start_clockwork()

  sm.send(started.data, StateTimeoutIn(60))
  sm.send(started.data, Move(Beta))
  process.sleep(200)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  discard(started.pid)
}

pub fn a_state_timeout_armed_by_a_transition_survives_it_test() -> Nil {
  // The ordering corner inside `changed_state`: the cancellation belongs to
  // the state being left, so it has to happen before the step's own timer
  // actions run. Backoff-with-a-retry is written exactly like this, and if
  // the two were the other way round it would never retry.
  let started = start_clockwork()

  sm.send(started.data, MoveArming(Beta, 40))
  process.sleep(150)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["state"]

  discard(started.pid)
}

pub fn a_state_timeout_that_already_fired_is_flushed_on_a_transition_test() -> Nil {
  // Zero milliseconds and then a sleep, so the fire is certainly sitting in
  // the mailbox when the transition cancels it. `erlang:cancel_timer` cannot
  // take a delivered message back; only the generation stamp can tell the
  // loop that the message it is about to read describes a state it has
  // already left.
  let started = start_clockwork()

  sm.send(started.data, Trip(Beta))
  process.sleep(200)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  discard(started.pid)
}

pub fn the_flush_control_delivers_the_same_fire_when_the_state_is_kept_test() -> Nil {
  // The control for the test above, and the reason it is worth anything.
  // Same zero-millisecond timer, same sleep, same injected transition — but
  // to the state the machine is already in, so nothing is cancelled and the
  // fire that was already in the mailbox is handled. Without this, a timer
  // that silently failed to arm would pass the flush test.
  let started = start_clockwork()

  sm.send(started.data, Trip(Alpha))
  process.sleep(200)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["state"]

  discard(started.pid)
}

// ---------------------------------------------------------- event timeouts

pub fn an_event_timeout_fires_after_a_quiet_stretch_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, EventTimeoutIn(40))
  process.sleep(150)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["event"]

  discard(started.pid)
}

pub fn an_event_timeout_is_cancelled_by_any_event_test() -> Nil {
  // The machine cancels it, not the handler: `Quiet` arms nothing and
  // re-arms nothing, and the deadline is gone all the same.
  let started = start_clockwork()

  sm.send(started.data, EventTimeoutIn(60))
  sm.send(started.data, Quiet)
  process.sleep(200)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  discard(started.pid)
}

pub fn an_event_timeout_is_kept_alive_by_re_arming_test() -> Nil {
  // Traffic every 20ms for 120ms, each message arming the deadline again.
  // The machine is never quiet for the 60ms the deadline wants, so it must
  // not ring — including in the instant a message and a fire could cross,
  // which is where the flush earns its keep.
  let started = start_clockwork()

  list.each(list.repeat(Nil, 6), fn(_attempt) {
    sm.send(started.data, EventTimeoutIn(60))
    process.sleep(20)
  })
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  // The `Rings` call above was itself an event, so the deadline is gone; arm
  // it once more and let the machine actually go quiet.
  sm.send(started.data, EventTimeoutIn(40))
  process.sleep(200)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["event"]

  discard(started.pid)
}

// ---------------------------------------------------------- named timeouts

pub fn a_named_timeout_survives_transitions_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, NamedTimeoutIn("dinner", 60))
  sm.send(started.data, Move(Beta))
  sm.send(started.data, Move(Gamma))
  process.sleep(250)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["dinner"]

  discard(started.pid)
}

pub fn a_named_timeout_is_cancelled_by_its_name_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, NamedTimeoutIn("dinner", 60))
  sm.send(started.data, CancelNamed("dinner"))
  process.sleep(250)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  discard(started.pid)
}

pub fn named_timeouts_are_independent_of_each_other_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, NamedTimeoutIn("dinner", 60))
  sm.send(started.data, NamedTimeoutIn("supper", 60))
  sm.send(started.data, CancelNamed("dinner"))
  process.sleep(250)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["supper"]

  discard(started.pid)
}

// ------------------------------------------------------------ debug plane

/// `sys:get_status/1` has no Gleam binding. The observer calls it, so a test
/// that wants to prove the observer would render this machine has to call it
/// the same way.
@external(erlang, "sys", "get_status")
fn get_status(pid: Pid) -> Dynamic

/// Read an atom out of a nested position in a raw Erlang term.
fn atom_at(data: Dynamic, path: List(Int)) -> String {
  let assert Ok(value) = decode.run(data, decode.at(path, atom.decoder()))
    as "the status reply must have an atom at this position"
  atom.to_string(value)
}

pub fn get_state_reports_the_state_and_the_data_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, Move(Beta))
  sm.send(started.data, Rang("hello"))

  // Sync on a call so that both events are certainly handled before the pair
  // is read out of band.
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["hello"]
  assert view(started.pid) == #("beta", ["hello"])

  discard(started.pid)
}

pub fn get_status_replies_in_the_shape_the_observer_reads_test() -> Nil {
  let started = start_clockwork()
  let status = get_status(started.pid)

  // `{status, Pid, {module, Module}, [PDict, Mode, Parent, Debug, Format]}`
  // is what `sys:get_status/1` specifies and what the observer's process
  // view renders.
  assert atom_at(status, [0]) == "status"
  assert atom_at(status, [2, 0]) == "module"
  assert atom_at(status, [2, 1]) == "weft@state_machine"
  assert atom_at(status, [3, 1]) == "running"

  let assert Ok(parent) = decode.run(status, decode.at([3, 2], decode.dynamic))
    as "the status report names the parent"
  assert string.inspect(parent) == string.inspect(process.self())

  discard(started.pid)
}

pub fn a_suspended_machine_serves_only_the_debug_plane_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, Rang("first"))
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["first"]

  system.suspend(started.pid)

  // The event is now stuck in the mailbox behind the suspension, so the pair
  // read out of band must still be the old one.
  sm.send(started.data, Rang("second"))
  process.sleep(50)
  assert view(started.pid) == #("alpha", ["first"])

  system.resume(started.pid)
  assert sm.call(started.data, waiting: 1000, sending: Rings)
    == ["first", "second"]

  discard(started.pid)
}

pub fn suspension_freezes_the_machine_s_timers_too_test() -> Nil {
  // A suspended machine is frozen, not quiet: a deadline that elapsed
  // entirely inside the suspension describes a stretch the machine was never
  // allowed to act in, so it is disarmed on the way in and restarted from
  // full on the way out.
  let started = start_clockwork()

  sm.send(started.data, NamedTimeoutIn("dinner", 120))
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  system.suspend(started.pid)
  process.sleep(300)

  // Two facts at once: nothing rang during the freeze, and the machine is
  // answering again the instant it resumes.
  system.resume(started.pid)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  // And the timer was put back rather than dropped.
  process.sleep(300)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["dinner"]

  discard(started.pid)
}

// -------------------------------------------------------------- shutdown

pub fn stop_shuts_the_machine_down_test() -> Nil {
  let assert Ok(started) =
    sm.new(Alpha, []) |> sm.on_event(holding_in([])) |> sm.start
    as "the machine must start"

  process.unlink(started.pid)
  sm.send(started.data, Halt)

  process.sleep(50)
  assert !process.is_alive(started.pid)
}

pub fn a_trapped_parent_exit_shuts_the_machine_down_test() -> Nil {
  let handoff = process.new_subject()

  // The machine is started by a process of its own so that the test can kill
  // its parent without killing itself.
  let parent =
    process.spawn_unlinked(fn() {
      let assert Ok(started) =
        sm.new(Alpha, [])
        |> sm.trapping_exits(True)
        |> sm.on_event(holding_in([]))
        |> sm.start
        as "the machine must start"

      process.send(handoff, started.pid)
      process.sleep_forever()
    })

  let assert Ok(machine) = process.receive(handoff, 1000)
    as "the intermediate parent must hand the machine over"

  process.kill(parent)

  // The exit signal reaches the machine as a message because it traps, and
  // the parent's exit takes a child with it whatever the reason.
  process.sleep(100)
  assert !process.is_alive(machine)
}

pub fn a_suspended_machine_still_shuts_down_on_a_parent_exit_test() -> Nil {
  // A supervisor terminating a suspended child must not have to wait out its
  // shutdown timeout: the frozen loop still watches for exits.
  let handoff = process.new_subject()

  let parent =
    process.spawn_unlinked(fn() {
      let assert Ok(started) =
        sm.new(Alpha, [])
        |> sm.trapping_exits(True)
        |> sm.on_event(holding_in([]))
        |> sm.start
        as "the machine must start"

      process.send(handoff, started.pid)
      process.sleep_forever()
    })

  let assert Ok(machine) = process.receive(handoff, 1000)
    as "the intermediate parent must hand the machine over"

  system.suspend(machine)
  process.kill(parent)

  process.sleep(100)
  assert !process.is_alive(machine)
}

// -------------------------------------------------------- upstream parity

pub fn a_named_machine_is_reachable_by_its_name_test() -> Nil {
  let name = process.new_name("weft_statem_named")

  let assert Ok(started) =
    sm.new(Alpha, [])
    |> sm.named(name)
    |> sm.on_event(holding_in([]))
    |> sm.start
    as "the machine must start"

  let subject = process.named_subject(name)
  sm.send(subject, Note("hello"))
  assert sm.call(subject, waiting: 1000, sending: Report) == ["hello"]

  discard(started.pid)
}

pub fn a_weft_machine_runs_and_restarts_under_a_gleam_otp_supervisor_test() -> Nil {
  let name = process.new_name("weft_statem_supervised")

  let child =
    sm.new(Alpha, [])
    |> sm.named(name)
    |> sm.on_event(holding_in([]))
    |> sm.supervised

  let assert Ok(started) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(child)
    |> supervisor.start
    as "the supervisor must start its weft child"

  let subject = process.named_subject(name)
  sm.send(subject, Note("first"))
  assert sm.call(subject, waiting: 1000, sending: Report) == ["first"]

  let assert Ok(first) = process.named(name)
    as "a started machine is registered under its name"

  // Killing the child and finding a fresh one under the same name is what
  // proves `supervised` produced a real child specification rather than a
  // one-shot start: a supervisor that could not restart it would leave the
  // name unregistered.
  process.kill(first)
  process.sleep(200)

  let assert Ok(second) = process.named(name)
    as "the supervisor must put a fresh machine back under the name"
  assert first != second
  assert sm.call(process.named_subject(name), waiting: 1000, sending: Report)
    == []

  discard(started.pid)
}

pub fn an_initialiser_error_is_reported_as_init_failed_test() -> Nil {
  let result =
    sm.new_with_initialiser(1000, fn(_subject) { Error("no socket") })
    |> sm.on_event(holding_in([]))
    |> sm.start

  assert result == Error(otp_actor.InitFailed("no socket"))
}
