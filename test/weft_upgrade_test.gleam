//// In-place migrations keep the recipient and its queued work alive.

import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/system
import gleeunit/should
import weft/actor
import weft/state_machine as sm
import weft/timer as weft_timer
import weft/upgrade

@external(erlang, "sys", "change_code")
fn change_code(pid: Pid, module: Atom, old: String, extra: String) -> Dynamic

@external(erlang, "sys", "get_state")
fn get_state(pid: Pid) -> Dynamic

fn change(pid: Pid, extra: String) -> Dynamic {
  change_code(pid, atom.create("weft_upgrade_test"), "v1", extra)
}

fn ok(reply: Dynamic) -> Nil {
  decode.run(reply, atom.decoder())
  |> should.equal(Ok(atom.create("ok")))
}

fn rejected(reply: Dynamic) -> Nil {
  decode.run(reply, decode.at([0], atom.decoder()))
  |> should.equal(Ok(atom.create("error")))
}

type Message {
  Add
  Hold
  Switch
  Stop
  Read(Subject(Int))
}

fn actor_handler(
  step: Int,
) -> fn(#(Int, Subject(Message)), Message) ->
  actor.Next(#(Int, Subject(Message)), Message) {
  fn(state: #(Int, Subject(Message)), message) {
    case message {
      Stop -> actor.stop()
      Add | Hold | Switch -> actor.continue(#(state.0 + step, state.1))
      Read(reply) -> {
        process.send(reply, state.0)
        actor.continue(state)
      }
    }
  }
}

fn actor_migrate() -> fn(upgrade.Request, #(Int, Subject(Message))) ->
  Result(actor.Migration(#(Int, Subject(Message)), Message), String) {
  fn(request: upgrade.Request, state: #(Int, Subject(Message))) {
    case decode.run(request.extra, decode.string) {
      Ok("reject") -> Error("refused")
      Ok("crash") -> panic as "migration crash regression"
      Ok("timeout") -> {
        process.sleep(200)
        Error("too late")
      }
      Ok("up") ->
        Ok(actor.Migration(
          state: #(state.0 + 100, state.1),
          on_message: actor_handler(10),
          on_shutdown: None,
          selector: process.new_selector() |> process.select(state.1),
          migrate: actor_migrate(),
        ))
      Ok("down") ->
        Ok(actor.Migration(
          state: #(state.0 - 100, state.1),
          on_message: actor_handler(1),
          on_shutdown: None,
          selector: process.new_selector() |> process.select(state.1),
          migrate: actor_migrate(),
        ))
      Ok(_) | Error(_) -> Error("unsupported version")
    }
  }
}

fn start_actor() -> #(Pid, Subject(Message)) {
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      Ok(actor.initialised(#(0, subject)) |> actor.returning(subject))
    })
    |> actor.on_message(actor_handler(1))
    |> actor.with_upgrade(within: 30, migrate: actor_migrate())
    |> actor.start
    as "the test actor starts"
  #(started.pid, started.data)
}

fn read(subject: Subject(Message)) -> Int {
  let reply = process.new_subject()
  process.send(subject, Read(reply))
  let assert Ok(state) = process.receive(reply, 1000)
    as "the original recipient replies"
  state
}

fn discard(pid: Pid) -> Nil {
  process.unlink(pid)
  process.kill(pid)
}

pub fn actor_upgrade_and_downgrade_transform_current_state_test() -> Nil {
  let #(pid, inbox) = start_actor()
  process.send(inbox, Add)
  read(inbox) |> should.equal(1)
  system.suspend(pid)
  process.send(inbox, Add)
  change(pid, "up") |> ok
  system.resume(pid)
  read(inbox) |> should.equal(111)
  process.send(inbox, Add)
  read(inbox) |> should.equal(121)
  system.suspend(pid)
  change(pid, "down") |> ok
  process.send(inbox, Add)
  system.resume(pid)
  read(inbox) |> should.equal(22)
  process.is_alive(pid) |> should.be_true
  discard(pid)
}

pub fn actor_failed_migrations_retain_state_callbacks_and_queued_work_test() -> Nil {
  let #(pid, inbox) = start_actor()
  use mode <- list.each(["reject", "crash", "timeout"])
  let before = read(inbox)
  system.suspend(pid)
  process.send(inbox, Add)
  change(pid, mode) |> rejected
  decode.run(get_state(pid), decode.at([0], decode.int))
  |> should.equal(Ok(before))
  system.resume(pid)
  read(inbox) |> should.equal(before + 1)
  case mode {
    "timeout" -> discard(pid)
    _ -> Nil
  }
}

pub fn code_change_requires_opt_in_and_suspension_test() -> Nil {
  let #(pid, inbox) = start_actor()
  change(pid, "up") |> rejected
  read(inbox) |> should.equal(0)
  discard(pid)
  let assert Ok(started) = actor.new(0) |> actor.start
    as "the non-opted actor starts"
  system.suspend(started.pid)
  change(started.pid, "up") |> rejected
  system.resume(started.pid)
  discard(started.pid)
}

fn event_handler(
  step: Int,
) -> fn(Int, #(Int, Subject(Message)), Message) ->
  sm.Next(Int, #(Int, Subject(Message)), Message) {
  fn(state, data: #(Int, Subject(Message)), message) {
    case message {
      Stop -> sm.stop()
      Add -> sm.transition(state, #(data.0 + step, data.1))
      Hold ->
        case state {
          0 -> sm.keep(data) |> sm.postpone
          _ -> sm.keep(#(data.0 + step, data.1))
        }
      Switch -> sm.transition(state + 1, data)
      Read(reply) -> {
        process.send(reply, data.0)
        sm.transition(state, data)
      }
    }
  }
}

fn machine_migrate() -> fn(upgrade.Request, Int, #(Int, Subject(Message))) ->
  Result(sm.Migration(Int, #(Int, Subject(Message)), Message), String) {
  fn(request: upgrade.Request, state: Int, data: #(Int, Subject(Message))) {
    case decode.run(request.extra, decode.string) {
      Ok("crash") -> panic as "machine migration crash regression"
      Ok("timeout") -> {
        process.sleep(200)
        Error("too late")
      }
      Ok("up") ->
        Ok(sm.Migration(
          state: state,
          data: #(data.0 + 100, data.1),
          on_event: event_handler(10),
          on_enter: Some(fn(_from, _to, data: #(Int, Subject(Message))) {
            sm.keep(#(data.0 + 1000, data.1))
          }),
          selector: process.new_selector() |> process.select(data.1),
          migrate: machine_migrate(),
        ))
      Ok("down") ->
        Ok(sm.Migration(
          state: state,
          data: #(data.0 - 100, data.1),
          on_event: event_handler(1),
          on_enter: None,
          selector: process.new_selector() |> process.select(data.1),
          migrate: machine_migrate(),
        ))
      Ok(_) | Error(_) -> Error("refused")
    }
  }
}

pub fn machine_migration_preserves_current_data_pid_and_queued_work_test() -> Nil {
  let assert Ok(started) =
    sm.new_with_initialiser(1000, fn(subject) {
      Ok(sm.initialised(0, #(0, subject)) |> sm.returning(subject))
    })
    |> sm.on_event(event_handler(1))
    |> sm.with_upgrade(within: 30, migrate: machine_migrate())
    |> sm.start
    as "the test machine starts"
  let inbox = started.data
  system.suspend(started.pid)
  process.send(inbox, Add)
  change(started.pid, "up") |> ok
  system.resume(started.pid)
  read(inbox) |> should.equal(110)
  system.suspend(started.pid)
  change(started.pid, "reject") |> rejected
  change(started.pid, "down") |> ok
  process.send(inbox, Add)
  system.resume(started.pid)
  read(inbox) |> should.equal(11)
  discard(started.pid)
}

pub fn machine_postponed_events_replay_under_new_callbacks_test() -> Nil {
  let assert Ok(started) =
    sm.new_with_initialiser(1000, fn(subject) {
      Ok(sm.initialised(0, #(0, subject)) |> sm.returning(subject))
    })
    |> sm.on_event(event_handler(1))
    |> sm.with_upgrade(within: 30, migrate: machine_migrate())
    |> sm.start
    as "the postponed-event machine starts"
  process.send(started.data, Hold)
  read(started.data) |> should.equal(0)
  system.suspend(started.pid)
  change(started.pid, "up") |> ok
  process.send(started.data, Switch)
  system.resume(started.pid)
  read(started.data) |> should.equal(1110)
  discard(started.pid)
}

pub fn machine_failed_migrations_resume_old_callbacks_test() -> Nil {
  let assert Ok(started) =
    sm.new_with_initialiser(1000, fn(subject) {
      Ok(sm.initialised(0, #(0, subject)) |> sm.returning(subject))
    })
    |> sm.on_event(event_handler(1))
    |> sm.with_upgrade(within: 30, migrate: machine_migrate())
    |> sm.start
    as "the failure-path machine starts"
  use mode <- list.each(["reject", "crash", "timeout"])
  let before = read(started.data)
  system.suspend(started.pid)
  process.send(started.data, Add)
  change(started.pid, mode) |> rejected
  system.resume(started.pid)
  read(started.data) |> should.equal(before + 1)
  case mode {
    "timeout" -> discard(started.pid)
    _ -> Nil
  }
}

type CallbackState {
  CallbackState(
    value: Int,
    inbox: Subject(Message),
    alternate: Subject(Message),
    shutdown: Subject(Int),
  )
}

fn callback_handler(
  step: Int,
) -> fn(CallbackState, Message) -> actor.Next(CallbackState, Message) {
  fn(state: CallbackState, message) {
    case message {
      Add | Hold | Switch ->
        actor.continue(CallbackState(..state, value: state.value + step))
      Stop -> actor.stop()
      Read(reply) -> {
        process.send(reply, state.value)
        actor.continue(state)
      }
    }
  }
}

fn callbacks_migrate(
  request: upgrade.Request,
  state: CallbackState,
) -> Result(actor.Migration(CallbackState, Message), String) {
  case decode.run(request.extra, decode.string) {
    Ok("up") ->
      Ok(actor.Migration(
        state:,
        on_message: callback_handler(10),
        on_shutdown: Some(fn(current: CallbackState, _reason) {
          process.send(current.shutdown, current.value)
        }),
        selector: process.new_selector()
          |> process.select(state.inbox)
          |> process.select(state.alternate),
        migrate: callbacks_migrate,
      ))
    Ok(_) | Error(_) -> Error("refused")
  }
}

pub fn actor_migration_replaces_selector_and_shutdown_callbacks_test() -> Nil {
  let shutdown = process.new_subject()
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(inbox) {
      let alternate = process.new_subject()
      Ok(
        actor.initialised(CallbackState(0, inbox, alternate, shutdown))
        |> actor.returning(#(inbox, alternate)),
      )
    })
    |> actor.on_message(callback_handler(1))
    |> actor.on_shutdown(fn(_state, _reason) { process.send(shutdown, -1) })
    |> actor.with_upgrade(within: 30, migrate: callbacks_migrate)
    |> actor.start
    as "the callback fixture starts"
  let #(inbox, alternate) = started.data
  system.suspend(started.pid)
  process.send(alternate, Add)
  change(started.pid, "up") |> ok
  system.resume(started.pid)
  // The alternate subject was absent from the original selector.
  process.send(inbox, Add)
  read(inbox) |> should.equal(20)
  process.send(inbox, Stop)
  process.receive(shutdown, 1000) |> should.equal(Ok(20))
}

pub fn timeout_kills_migration_worker_before_old_behavior_resumes_test() -> Nil {
  let worker = process.new_subject()
  let late = process.new_subject()
  let assert Ok(started) =
    actor.new(0)
    |> actor.with_upgrade(within: 30, migrate: fn(_request, _state) {
      // Instrumentation deliberately has effects; production migrations must not.
      process.send(worker, process.self())
      process.sleep(150)
      process.send(late, 1)
      Error("late")
    })
    |> actor.start
    as "the timeout fixture starts"
  system.suspend(started.pid)
  change(started.pid, "up") |> rejected
  let assert Ok(worker_pid) = process.receive(worker, 1000)
    as "the worker published its identity"
  process.is_alive(worker_pid) |> should.be_false
  system.resume(started.pid)
  process.receive(late, 180) |> should.equal(Error(Nil))
  process.is_alive(started.pid) |> should.be_true
  discard(started.pid)
}

pub fn actor_injected_work_survives_callback_migration_test() -> Nil {
  let handling = process.new_subject()
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      Ok(actor.initialised(#(0, subject)) |> actor.returning(subject))
    })
    |> actor.on_message(fn(state: #(Int, Subject(Message)), message) {
      case message {
        Hold -> {
          process.send(handling, Nil)
          actor.continue(#(state.0 + 1, state.1)) |> actor.then_handle(Add)
        }

        // Keep one injected message pending until the system plane suspends
        // the loop. The assertion reads the frozen count, so scheduling the
        // controller later changes no expected ordering or result.
        Add -> actor.continue(#(state.0 + 1, state.1)) |> actor.then_handle(Add)
        Switch -> actor.continue(#(state.0 + 1, state.1))
        Stop -> actor.stop()
        Read(reply) -> {
          process.send(reply, state.0)
          actor.continue(state)
        }
      }
    })
    |> actor.with_upgrade(within: 30, migrate: actor_migrate())
    |> actor.start
    as "the injected-work fixture starts"
  process.send(started.data, Hold)
  process.receive(handling, 1000) |> should.equal(Ok(Nil))
  system.suspend(started.pid)
  let assert Ok(before) =
    decode.run(get_state(started.pid), decode.at([0], decode.int))
    as "the suspended actor reports its populated count"
  change(started.pid, "up") |> ok
  system.resume(started.pid)
  read(started.data) |> should.equal(before + 110)
  discard(started.pid)
}

pub fn actor_periodic_timer_resumes_with_new_callbacks_test() -> Nil {
  let wakes = process.new_subject()
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(subject) {
      Ok(actor.initialised(#(0, subject)) |> actor.returning(subject))
    })
    |> actor.on_message(actor_handler(1))
    |> actor.periodic(every: 100, sending: Add)
    |> actor.with_timer_source(
      weft_timer.Injected(fn(_delay, wake) { process.send(wakes, wake) }),
    )
    |> actor.with_upgrade(within: 30, migrate: actor_migrate())
    |> actor.start
    as "the periodic fixture starts"
  let assert Ok(old_wake) = process.receive(wakes, 1000)
    as "the initial periodic timer is armed"
  system.suspend(started.pid)
  old_wake()
  change(started.pid, "up") |> ok
  system.resume(started.pid)
  let assert Ok(new_wake) = process.receive(wakes, 1000)
    as "resume restores the periodic timer"
  read(started.data) |> should.equal(100)
  new_wake()
  read(started.data) |> should.equal(110)
  process.receive(wakes, 1000) |> should.be_ok
  discard(started.pid)
}
