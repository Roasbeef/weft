//// Terminal exits must retain their reason even when the loop traps exits.
//// Monitors are installed before the stop trigger, so Normal cannot be
//// mistaken for a late monitor's noproc report.

import gleam/dynamic
import gleam/erlang/process
import weft/actor
import weft/state_machine as sm

type Command {
  StopNormally
  StopAbnormally
  LinkTo(process.Pid, process.Subject(Nil))
}

fn start_actor() -> #(process.Pid, process.Subject(Command)) {
  let assert Ok(started) =
    actor.new(Nil)
    |> actor.trapping_exits(True)
    |> actor.unlinked
    |> actor.on_message(fn(_state, message) {
      case message {
        StopNormally -> actor.stop()
        StopAbnormally -> actor.stop_abnormal("terminal_failure")
        LinkTo(pid, ready) -> {
          process.link(pid)
          process.send(ready, Nil)
          actor.continue(Nil)
        }
      }
    })
    |> actor.start
    as "the trapping actor must start"
  #(started.pid, started.data)
}

fn start_machine() -> #(process.Pid, process.Subject(Command)) {
  let assert Ok(started) =
    sm.new(Nil, Nil)
    |> sm.trapping_exits(True)
    |> sm.unlinked
    |> sm.on_event(fn(_state, _data, message) {
      case message {
        StopNormally -> sm.stop()
        StopAbnormally -> sm.stop_abnormal("terminal_failure")
        LinkTo(pid, ready) -> {
          process.link(pid)
          process.send(ready, Nil)
          sm.keep(Nil)
        }
      }
    })
    |> sm.start
    as "the trapping machine must start"
  #(started.pid, started.data)
}

fn watching(pid: process.Pid) -> process.Selector(process.ExitReason) {
  process.new_selector()
  |> process.select_specific_monitor(process.monitor(pid), fn(down) {
    case down {
      process.ProcessDown(reason:, ..) -> reason
      process.PortDown(reason:, ..) -> reason
    }
  })
}

fn check_stop(
  started: #(process.Pid, process.Subject(Command)),
  command: Command,
  expected: process.ExitReason,
) -> Nil {
  let #(pid, subject) = started
  let monitor = watching(pid)
  process.send(subject, command)
  let actual = process.selector_receive(monitor, 1000)

  // Reap a broken implementation before an assertion aborts the test.
  process.kill(pid)
  assert actual == Ok(expected)
}

pub fn trapping_actor_abnormal_stop_preserves_reason_test() -> Nil {
  check_stop(
    start_actor(),
    StopAbnormally,
    process.Abnormal(dynamic.string("terminal_failure")),
  )
}

pub fn trapping_machine_abnormal_stop_preserves_reason_test() -> Nil {
  check_stop(
    start_machine(),
    StopAbnormally,
    process.Abnormal(dynamic.string("terminal_failure")),
  )
}

pub fn trapping_actor_normal_stop_stays_normal_test() -> Nil {
  check_stop(start_actor(), StopNormally, process.Normal)
}

pub fn trapping_machine_normal_stop_stays_normal_test() -> Nil {
  check_stop(start_machine(), StopNormally, process.Normal)
}

fn check_linked_exit(started: #(process.Pid, process.Subject(Command))) -> Nil {
  let #(pid, subject) = started
  let monitor = watching(pid)
  let ready = process.new_subject()
  let handoff = process.new_subject()
  let reason =
    dynamic.array([dynamic.string("linked_failure"), dynamic.int(42)])
  let child =
    process.spawn_unlinked(fn() {
      let trigger = process.new_subject()
      process.send(handoff, trigger)
      let assert Ok(Nil) = process.receive(trigger, 1000)
        as "the original monitor and link must precede the failure"
      exit_self(reason)
    })
  let assert Ok(trigger) = process.receive(handoff, 1000)
    as "the child must publish its own trigger subject"
  process.send(subject, LinkTo(child, ready))
  let assert Ok(Nil) = process.receive(ready, 1000)
    as "the linked child must be attached before it exits"
  process.send(trigger, Nil)
  let actual = process.selector_receive(monitor, 1000)
  process.kill(pid)
  process.kill(child)
  assert actual == Ok(process.Abnormal(reason))
}

pub fn trapping_actor_linked_failure_preserves_exact_term_test() -> Nil {
  check_linked_exit(start_actor())
}

pub fn trapping_machine_linked_failure_preserves_exact_term_test() -> Nil {
  check_linked_exit(start_machine())
}

@external(erlang, "erlang", "exit")
fn exit_self(reason: dynamic.Dynamic) -> Nil
