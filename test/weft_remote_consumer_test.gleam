//// Consumer cancellation observes remote monitors, rather than asking a local
//// liveness BIF to inspect a remote PID. The two-node fixture runs in its own
//// OS VM so ordinary tests never inherit distribution flags or node state.

import gleam/erlang/process.{type Pid}
import weft

/// A local consumer already gone cannot admit a worker even before DOWN dispatch.
pub fn local_already_dead_consumer_prevents_task_start_test() {
  let consumer = process.spawn_unlinked(fn() { Nil })
  let monitor = process.monitor(consumer)
  let assert Ok(_) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(2000)
    as "the local consumer exits before run admission"

  let started = process.new_subject()
  let account: List(weft.Outcome(Nil, Nil)) =
    weft.new([
      fn() {
        process.send(started, Nil)
        Ok(Nil)
      },
    ])
    |> weft.cancel_when_exits(consumer)
    |> weft.start

  assert account == [weft.NeverStarted(0)]
  assert process.receive(started, 0) == Error(Nil)
}

/// Independent real BEAM nodes prove remote death and disconnect cancellation.
pub fn real_remote_consumer_death_and_disconnect_cancel_test() {
  let assert Ok(Nil) = probe()
    as "the OS-node fixture completes both remote cancellation controls"
}

/// The fixed OS fixture calls this role with a genuine remote PID. A started
/// worker and its monitor establish admission before the remote loss occurs.
pub fn exercise(consumer: Pid, loss: Int) -> Nil {
  let started = process.new_subject()
  let detached =
    weft.new([
      fn() {
        process.send(started, process.self())
        process.sleep_forever()
        Ok(Nil)
      },
    ])
    |> weft.cancel_when_exits(consumer)
    |> weft.deadline(5000)
    |> weft.start_detached

  let assert Ok(worker) = process.receive(started, 2000)
    as "a genuine remote consumer permits worker admission"
  let monitor = process.monitor(worker)
  assert weft.pull(detached, within: 0) == weft.NotYet

  // The fixture's only loss operations are consumer exit and node disconnect.
  lose(consumer, loss)
  assert weft.pull(detached, within: 2000)
    == weft.PulledOutcome(weft.Abandoned(0))
  assert weft.pull(detached, within: 2000) == weft.AllDelivered
  let assert Ok(_) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(2000)
    as "cancellation joins the admitted worker before fixture completion"
  Nil
}

// gleam_erlang has no OS BEAM or OTP peer launcher. This test-only primitive
// owns finite, isolated node lifetime; production uses the existing monitor.
@external(erlang, "weft_remote_consumer_test_ffi", "probe")
fn probe() -> Result(Nil, Nil)

// No remote liveness oracle is used: these fixed test controls inject loss.
@external(erlang, "weft_remote_consumer_test_ffi", "lose")
fn lose(consumer: Pid, loss: Int) -> Nil
