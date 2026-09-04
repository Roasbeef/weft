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
import weft/timer

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

  /// Arm a periodic timeout that rings its own name over and over.
  PeriodicIn(name: String, ms: Int)

  /// Arm a periodic timeout and queue a `Doze` behind it, which is how a
  /// test forces a tick to be genuinely in flight when it is cancelled.
  /// `cancelling` decides whether the doze ends the series or leaves it
  /// alone, and is the single line the flush test and its control differ by.
  TripPeriodic(name: String, ms: Int, cancelling: Cancelling)

  /// Sleep long enough for the tick armed by `TripPeriodic` to have landed
  /// in the mailbox, then do what that arming asked for. Handled from the
  /// injected queue, so it runs before the loop looks at the mailbox at all.
  DozePeriodic(name: String, cancelling: Cancelling)

  /// Stop the machine, with whatever timers it still has armed.
  Cease

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

/// Whether a `TripPeriodic` doze ends the series it armed.
///
/// The flush test and its control are the same scenario with this value
/// flipped, which is the only way "the fire was flushed" says anything: a
/// timer that never fired at all would pass the flush test on its own.
type Cancelling {
  CancelIt
  LeaveIt
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

    PeriodicIn(name:, ms:) ->
      sm.keep(log)
      |> sm.with_periodic_timeout(name:, every: ms, sending: Rang(name))

    // The arm and the doze are one chain, so the doze is handled from the
    // injected queue — ahead of the mailbox the tick is sitting in. Without
    // it the cancel usually wins the race and the flush is never exercised.
    TripPeriodic(name:, ms:, cancelling:) ->
      sm.keep(log)
      |> sm.with_periodic_timeout(name:, every: ms, sending: Rang(name))
      |> sm.then_handle(DozePeriodic(name:, cancelling:))

    DozePeriodic(name:, cancelling:) -> {
      process.sleep(40)
      case cancelling {
        CancelIt -> sm.keep(log) |> sm.cancel_timeout(name:)
        LeaveIt -> sm.keep(log)
      }
    }

    Cease -> sm.stop()

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

// ------------------------------------------------------- periodic timeouts

pub fn a_periodic_timeout_rings_over_and_over_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, PeriodicIn("tick", 30))
  process.sleep(250)
  let rings = sm.call(started.data, waiting: 1000, sending: Rings)

  // A one-shot named timeout under the same name would give exactly one,
  // which is the failure this count is chosen to separate from.
  assert list.length(rings) >= 3
  assert list.all(rings, fn(name) { name == "tick" })

  discard(started.pid)
}

pub fn a_periodic_timeout_survives_state_changes_test() -> Nil {
  // The heartbeat that must keep beating while the machine moves between
  // phases is the whole reason this kind exists, so the transitions here
  // are the assertion rather than scenery.
  let started = start_clockwork()

  sm.send(started.data, PeriodicIn("tick", 30))
  sm.send(started.data, Move(Beta))
  sm.send(started.data, Move(Gamma))
  process.sleep(250)

  assert list.length(sm.call(started.data, waiting: 1000, sending: Rings)) >= 3
  assert view(started.pid).0 == "gamma"

  discard(started.pid)
}

pub fn a_periodic_timeout_is_cancelled_by_its_name_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, PeriodicIn("tick", 30))
  process.sleep(150)
  sm.send(started.data, CancelNamed("tick"))
  let rung = sm.call(started.data, waiting: 1000, sending: Rings)

  // It was running, and then it was not: the first assertion is what stops
  // the second from passing against a timer that never started.
  assert list.length(rung) >= 1
  process.sleep(250)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == rung

  discard(started.pid)
}

pub fn a_cancel_inside_a_handler_flushes_the_tick_in_flight_test() -> Nil {
  // The tick lands in the mailbox during the doze and the cancel runs after
  // it: the fire cannot be recalled, so the only thing that can keep it out
  // of the handler is the generation stamp the timer book drops it on.
  let started = start_clockwork()

  sm.send(started.data, TripPeriodic("tick", 5, CancelIt))
  process.sleep(250)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  discard(started.pid)
}

pub fn the_flush_control_delivers_the_tick_when_nothing_cancels_it_test() -> Nil {
  // The same scenario with `LeaveIt` for `CancelIt`. Without this, a
  // periodic timeout that silently never armed would pass the flush test.
  let started = start_clockwork()

  sm.send(started.data, TripPeriodic("tick", 5, LeaveIt))
  process.sleep(250)
  let rings = sm.call(started.data, waiting: 1000, sending: Rings)
  assert list.first(rings) == Ok("tick")

  discard(started.pid)
}

pub fn re_arming_a_periodic_timeout_replaces_its_interval_test() -> Nil {
  // Two armings of one name, fast then slow. If the second added a timer
  // rather than replacing one, the fast series would still be running and
  // the window below would be full of ticks.
  let started = start_clockwork()

  sm.send(started.data, PeriodicIn("tick", 20))
  sm.send(started.data, PeriodicIn("tick", 2000))
  process.sleep(250)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  discard(started.pid)
}

pub fn the_re_arm_control_shows_the_fast_interval_would_have_rung_test() -> Nil {
  let started = start_clockwork()

  sm.send(started.data, PeriodicIn("tick", 20))
  process.sleep(250)
  assert list.length(sm.call(started.data, waiting: 1000, sending: Rings)) >= 3

  discard(started.pid)
}

pub fn a_one_shot_named_timeout_replaces_a_periodic_one_test() -> Nil {
  // The two kinds share a name space, so this is a conversion rather than a
  // second timer — and the re-arm that runs after each tick must not put
  // the series back once the handler has said one-shot.
  let started = start_clockwork()

  sm.send(started.data, PeriodicIn("tick", 20))
  sm.send(started.data, NamedTimeoutIn("tick", 20))
  process.sleep(250)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["tick"]

  discard(started.pid)
}

pub fn a_periodic_timeout_dies_with_the_machine_test() -> Nil {
  // A machine that stops while a periodic timeout is armed stays stopped:
  // the timer fires into a subject its own dead process owned, and nothing
  // about the series outlives the exit.
  let started = start_clockwork()

  sm.send(started.data, PeriodicIn("tick", 20))
  process.sleep(80)
  sm.send(started.data, Cease)

  process.sleep(150)
  assert !process.is_alive(started.pid)
}

pub fn suspension_freezes_a_periodic_timeout_too_test() -> Nil {
  // A suspended machine is frozen, not quiet. The ticks it could not have
  // acted on are not owed to it on resume, so the series restarts from full
  // rather than delivering a backlog.
  let started = start_clockwork()

  sm.send(started.data, PeriodicIn("tick", 400))
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  system.suspend(started.pid)
  process.sleep(900)
  system.resume(started.pid)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  // And the series was put back rather than dropped.
  process.sleep(900)
  assert list.length(sm.call(started.data, waiting: 1000, sending: Rings)) >= 2

  discard(started.pid)
}

// ------------------------------------------------- an injected timer source

/// One arming the machine asked its injected timer source for.
///
/// The wake is kept rather than run: a machine on this source has no clock
/// of its own, so every fire in the two tests below is one the test decided
/// to deliver.
type Arming {
  Arming(delay_ms: Int, wake: fn() -> Nil)
}

/// A clockwork machine whose timeouts are armed through a fake wheel.
///
/// The wheel records and fires nothing, which is what makes these tests say
/// something: on the wall clock — every other timeout test in this file —
/// the same armings ring on their own after a sleep, and here no sleep is
/// long enough.
fn start_clockwork_on(
  armings: Subject(Arming),
) -> sm.Started(Subject(Clockwork)) {
  let source =
    timer.Injected(after: fn(delay_ms, wake) {
      process.send(armings, Arming(delay_ms:, wake:))
    })

  let assert Ok(started) =
    sm.new(Alpha, [])
    |> sm.on_event(clockwork_handler)
    |> sm.with_timer_source(source)
    |> sm.start
    as "the machine must start"
  started
}

pub fn an_injected_named_timeout_rings_only_when_woken_test() -> Nil {
  let armings = process.new_subject()
  let started = start_clockwork_on(armings)

  sm.send(started.data, NamedTimeoutIn("dinner", 40))

  let assert Ok(arming) = process.receive(armings, 500)
    as "the machine armed its named timeout through the injected source"
  assert arming.delay_ms == 40

  // The control is `a_named_timeout_survives_transitions_test` and every
  // other timeout test above: the same arming on the wall clock rings after
  // a sleep of this length. Here the wall clock buys nothing at all.
  process.sleep(200)
  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

  arming.wake()

  // The wake was sent before the call, from this process, so the fire is
  // ahead of the call in the machine's mailbox and is certainly handled
  // first. Nothing here is waiting on a race.
  assert sm.call(started.data, waiting: 1000, sending: Rings) == ["dinner"]

  discard(started.pid)
}

pub fn a_cancelled_injected_named_timeout_is_flushed_by_the_loop_test() -> Nil {
  // The same machine as above with one line added, which is what makes the
  // pair worth anything. The added line is the cancel — and under an
  // injected source a cancel stops nothing, so this is the only thing
  // standing between a cancelled timeout and the event handler.
  let armings = process.new_subject()
  let started = start_clockwork_on(armings)

  sm.send(started.data, NamedTimeoutIn("dinner", 40))

  let assert Ok(arming) = process.receive(armings, 500)
    as "the machine armed its named timeout through the injected source"

  sm.send(started.data, CancelNamed("dinner"))
  arming.wake()

  assert sm.call(started.data, waiting: 1000, sending: Rings) == []

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

// ------------------------------------------------- the connection manager

/// The canonical gen_statem example, which is the example on issue #2 and
/// the one that exercises postpone, the state timeout and the enter callback
/// at once.
type Link {
  Connecting
  Ready
  Backoff
}

/// The wire events. `Attempt` is injected by the enter callback rather than
/// sent by anyone, which is the point: every path into `Connecting` starts a
/// dial, including the ones added later.
type Wire {
  Attempt
  Retry
  Request(body: String)
  Served(reply: Subject(List(String)))
}

/// What the link carries across transitions: how many dials it has made, and
/// what it has served.
type Session {
  Session(attempts: Int, served: List(String))
}

/// The handler, written as `case state, message` so that the compiler names
/// every pair nobody has thought about. There is no catch-all arm anywhere
/// in it, which is the whole argument for a state ADT over a state atom.
fn link_handler(
  state: Link,
  data: Session,
  message: Wire,
) -> sm.Next(Link, Session, Wire) {
  case state, message {
    // A request that arrives before the link is up is neither an error nor a
    // special case: it waits inside the machine and is redelivered the
    // instant we reach Ready, in the order it arrived. The report waits with
    // it, so the caller sees a settled link rather than an empty one.
    Connecting, Request(..)
    | Backoff, Request(..)
    | Connecting, Served(..)
    | Backoff, Served(..)
    -> sm.keep(data) |> sm.postpone

    // The first dial fails and the second succeeds. What is being tested is
    // the shape of the transitions, not a network.
    Connecting, Attempt -> {
      let data = Session(..data, attempts: data.attempts + 1)
      case data.attempts == 1 {
        True -> sm.transition(to: Backoff, data:)
        False -> sm.transition(to: Ready, data:)
      }
    }

    Backoff, Retry -> sm.transition(to: Connecting, data:)

    Ready, Request(body:) ->
      sm.keep(Session(..data, served: [body, ..data.served]))

    Ready, Served(reply:) -> {
      process.send(reply, list.reverse(data.served))
      sm.keep(data)
    }

    // The pairs that cannot arise still have to be written down. That is the
    // exhaustiveness, and it is why adding a fourth state variant would stop
    // this file compiling until somebody decided what it means.
    Connecting, Retry | Ready, Retry | Ready, Attempt | Backoff, Attempt ->
      sm.keep(data)
  }
}

/// The enter callback: the effects that belong to arriving in a state rather
/// than to the event that got us there.
fn link_enter(
  _from: Link,
  to: Link,
  data: Session,
) -> sm.Enter(Link, Session, Wire) {
  case to {
    Connecting -> sm.keep(data) |> sm.then_handle(Attempt)

    // The retry is a state timeout, so it dies with the state: a stale Retry
    // can never be handled once something else has moved us out of Backoff.
    Backoff -> sm.keep(data) |> sm.with_state_timeout(after: 20, sending: Retry)

    Ready -> sm.keep(data)
  }
}

pub fn the_connection_manager_serves_queued_requests_in_order_test() -> Nil {
  let assert Ok(started) =
    sm.new_with_initialiser(1000, fn(subject) {
      // Three requests, all of them in the mailbox before the machine has
      // handled anything at all: the queueing is not a race the test has to
      // win.
      process.send(subject, Request("one"))
      process.send(subject, Request("two"))
      process.send(subject, Request("three"))
      sm.initialised(Connecting, Session(attempts: 0, served: []))
      |> sm.returning(subject)
      |> Ok
    })
    |> sm.on_event(link_handler)
    |> sm.on_enter(link_enter)
    |> sm.start
    as "the machine must start"

  // The initial enter call dials, the first dial fails into Backoff, the
  // requests pile up postponed, the state timeout retries, and the second
  // dial reaches Ready — where all three are replayed in arrival order.
  assert sm.call(started.data, waiting: 1000, sending: Served)
    == ["one", "two", "three"]

  discard(started.pid)
}

pub fn the_connection_manager_reaches_ready_test() -> Nil {
  let assert Ok(started) =
    sm.new(Connecting, Session(attempts: 0, served: []))
    |> sm.on_event(link_handler)
    |> sm.on_enter(link_enter)
    |> sm.start
    as "the machine must start"

  // Nothing external drives this: the enter callback dials, the state
  // timeout retries, and the machine settles in Ready on its own.
  process.sleep(200)
  let raw = system.get_state(started.pid)
  let assert Ok(state) = decode.run(raw, decode.at([0], atom.decoder()))
    as "the link's state is a nullary variant, so an atom"
  assert atom.to_string(state) == "ready"

  discard(started.pid)
}

// ------------------------------------------------------------ linkage

pub fn an_unlinked_machine_survives_its_starters_crash_test() -> Nil {
  // The starter is a throwaway process that dies abnormally right after
  // the start returns; a linked machine would die with it.
  let handoff = process.new_subject()
  process.spawn_unlinked(fn() {
    let assert Ok(started) =
      sm.new(Alpha, [])
      |> sm.on_event(clockwork_handler)
      |> sm.unlinked
      |> sm.start
      as "the unlinked machine must start"
    process.send(handoff, started.pid)
    process.kill(process.self())
  })
  let assert Ok(pid) = process.receive(handoff, 2000)
    as "the starter hands the machine's pid over"

  process.sleep(50)
  assert process.is_alive(pid)
  process.kill(pid)
}

// ------------------------------------------------------------ selectors

/// A machine that opens a side channel on demand.
type Rewire {
  Open
  Heard(word: String)
  Recount(reply: Subject(List(String)))
}

type Rewired {
  Rewired(own: Subject(Rewire), heard: List(String))
}

pub fn a_step_can_replace_the_selector_it_receives_with_test() -> Nil {
  // The machine starts receiving only on its own subject. `Open` creates a
  // channel that did not exist at initialisation — a subject the handler
  // itself makes — hands it out, and widens the selector to it; the
  // machine must then hear on both.
  let opened = process.new_subject()
  let assert Ok(started) =
    sm.new_with_initialiser(1000, fn(own) {
      sm.initialised(Alpha, Rewired(own:, heard: []))
      |> sm.returning(own)
      |> Ok
    })
    |> sm.on_event(fn(_state, data: Rewired, message: Rewire) {
      case message {
        Open -> {
          let side = process.new_subject()
          process.send(opened, side)
          sm.keep(data)
          |> sm.with_selector(
            process.new_selector()
            |> process.select(data.own)
            |> process.select_map(side, Heard),
          )
        }
        Heard(word:) -> sm.keep(Rewired(..data, heard: [word, ..data.heard]))
        Recount(reply:) -> {
          process.send(reply, data.heard)
          sm.keep(data)
        }
      }
    })
    |> sm.start
    as "the machine must start"

  process.send(started.data, Open)
  let assert Ok(side) = process.receive(opened, 2000)
    as "the handler hands the side channel out"
  process.send(side, "hello")
  assert sm.call(started.data, 1000, Recount) == ["hello"]
}
