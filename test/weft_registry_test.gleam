//// Registry lifetime and replacement tests over real OTP processes.
////
//// References remain stable while recipient subjects change. The tests
//// replay an actual old monitor message after replacement and count both
//// table rows and monitors, so stale cleanup and hidden bookkeeping growth
//// cannot pass on the strength of message delivery alone.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/erlang/reference
import gleam/list
import gleam/otp/actor as otp_actor
import weft/actor
import weft/internal/registry as internal_registry
import weft/poll
import weft/registry
import weft/state_machine as sm

type Probe {
  Read(reply: Subject(Int))
  Stop
}

type Down

type Registration

@external(erlang, "weft_registry_test_ffi", "suspend")
fn suspend(names: registry.Registry) -> Nil

@external(erlang, "weft_registry_test_ffi", "resume")
fn resume(names: registry.Registry) -> Nil

@external(erlang, "weft_registry_test_ffi", "caller_monitor_count")
fn caller_monitor_count() -> Int

@external(erlang, "weft_registry_test_ffi", "queue_registration")
fn queue_registration(
  address: registry.Address(message),
  subject: Subject(message),
) -> Registration

@external(erlang, "weft_registry_test_ffi", "finish_registration")
fn finish_registration(
  names: registry.Registry,
  request: Registration,
) -> Result(Nil, String)

@external(erlang, "weft_registry_test_ffi", "stats")
fn stats(registry: registry.Registry) -> #(Int, Int)

@external(erlang, "weft_registry_test_ffi", "first_down")
fn first_down(registry: registry.Registry) -> Down

@external(erlang, "weft_registry_test_ffi", "replay_down")
fn replay_down(registry: registry.Registry, down: Down) -> Nil

@external(erlang, "weft_registry_test_ffi", "atom_count")
fn atom_count() -> Int

fn recipient(
  address: registry.Address(Probe),
  value: Int,
) -> actor.StartResult(Subject(Probe)) {
  actor.new(value)
  |> actor.addressed(address)
  |> actor.on_message(fn(value, message) {
    case message {
      Read(reply) -> {
        process.send(reply, value)
        actor.continue(value)
      }
      Stop -> actor.stop()
    }
  })
  |> actor.start
}

fn read(address: registry.Address(Probe)) -> Int {
  let assert Ok(inbox) = registry.lookup(address)
    as "the address must resolve to the current recipient"
  actor.call(inbox, 1000, Read)
}

fn kill_and_join(pid: Pid) -> Nil {
  process.unlink(pid)
  let monitor = process.monitor(pid)
  process.kill(pid)
  let assert Ok(_) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(1000)
    as "recipient death must precede the replacement attempt"
  Nil
}

pub fn a_reference_resolves_the_replacement_not_a_cached_pid_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let assert Ok(first) = recipient(address, 11)
    as "the first recipient must start"
  assert read(address) == 11
  kill_and_join(first.pid)
  assert registry.lookup(address) == Error(Nil)

  let assert Ok(second) = recipient(address, 22)
    as "the same address must accept a replacement after death"
  assert read(address) == 22
  assert first.data != second.data
  kill_and_join(second.pid)
  assert registry.stop(names) == Ok(Nil)
}

pub fn duplicate_binding_is_idempotent_but_another_subject_conflicts_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let original = process.new_subject()
  let competitor = process.new_subject()
  assert registry.register(address, original) == Ok(Nil)
  list.each(list.repeat(Nil, 100), fn(_) {
    assert registry.register(address, original) == Ok(Nil)
  })
  assert stats(names) == #(1, 1)
  assert registry.register(address, competitor)
    == Error("address already registered")
  assert registry.lookup(address) == Ok(original)
  assert registry.stop(names) == Ok(Nil)
}

pub fn registration_timeout_releases_the_callers_monitor_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let key = reference.new()
  let subject = process.new_subject()
  let before = caller_monitor_count()
  suspend(names)
  list.each(list.repeat(Nil, 3), fn(_) {
    assert internal_registry.register_within(names, key, subject, 5)
      == Error("registry unavailable")
    assert caller_monitor_count() == before
  })
  resume(names)
  assert stats(names) == #(1, 1)
  assert registry.stop(names) == Ok(Nil)
}

pub fn stale_down_cannot_remove_a_replacement_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let assert Ok(first) = recipient(address, 1)
    as "the first recipient must start"
  let stale = first_down(names)
  kill_and_join(first.pid)

  let assert Ok(second) = recipient(address, 2)
    as "the replacement must bind after the first recipient exits"
  replay_down(names, stale)

  // The stats request follows the replay from the same sender. Its reply
  // proves cleanup was handled before testing the replacement binding.
  assert stats(names) == #(1, 1)
  assert read(address) == 2
  kill_and_join(second.pid)
  assert registry.stop(names) == Ok(Nil)
}

pub fn actor_registration_precedes_initialisation_and_start_ack_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let assert Ok(started) =
    actor.new_with_initialiser(1000, fn(inbox) {
      assert registry.lookup(address) == Ok(inbox)
      actor.initialised(Nil)
      |> actor.returning(inbox)
      |> Ok
    })
    |> actor.addressed(address)
    |> actor.start
    as "registration must be complete before initialisation"
  assert registry.lookup(address) == Ok(started.data)
  kill_and_join(started.pid)
  assert registry.stop(names) == Ok(Nil)
}

pub fn replacement_can_bind_before_the_old_down_is_handled_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let assert Ok(first) = recipient(address, 1)
    as "the first recipient must start"
  let stale = first_down(names)
  let replacement = process.new_subject()
  let pending = queue_registration(address, replacement)

  // The registry is suspended with registration already queued. Death
  // therefore leaves the old row present when that callback runs first.
  kill_and_join(first.pid)
  assert finish_registration(names, pending) == Ok(Nil)
  replay_down(names, stale)
  assert stats(names) == #(1, 1)
  assert registry.lookup(address) == Ok(replacement)
  let reply = process.new_subject()
  assert registry.send(address, Read(reply)) == Ok(Nil)
  assert process.receive(replacement, 1000) == Ok(Read(reply))
  assert registry.stop(names) == Ok(Nil)
}

pub fn conflicting_start_never_runs_its_initialiser_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let assert Ok(first) = recipient(address, 1)
    as "the first recipient must start"
  let initialised = process.new_subject()
  let result =
    actor.new_with_initialiser(1000, fn(inbox) {
      process.send(initialised, Nil)
      actor.initialised(Nil)
      |> actor.returning(inbox)
      |> Ok
    })
    |> actor.addressed(address)
    |> actor.start
  assert result == Error(otp_actor.InitFailed("address already registered"))
  assert process.receive(initialised, 0) == Error(Nil)
  assert read(address) == 1
  kill_and_join(first.pid)
  assert registry.stop(names) == Ok(Nil)
}

pub fn failed_initialisation_releases_the_binding_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let result =
    actor.new_with_initialiser(1000, fn(_) { Error("refused") })
    |> actor.addressed(address)
    |> actor.start
  assert result == Error(otp_actor.InitFailed("refused"))
  assert poll.until(within: 1000, every: 1, attempt: fn() {
      case stats(names) {
        #(0, 0) -> poll.Done(Nil)
        _ -> poll.Retry
      }
    })
    == poll.Answered(Nil)
  let assert Ok(replacement) = recipient(address, 2)
    as "a failed initialiser must not retain registration"
  assert read(address) == 2
  kill_and_join(replacement.pid)
  assert registry.stop(names) == Ok(Nil)
}

pub fn state_machine_replacement_uses_the_same_address_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let start = fn(value) {
    sm.new_with_initialiser(1000, fn(inbox) {
      assert registry.lookup(address) == Ok(inbox)
      sm.initialised(Nil, value)
      |> sm.returning(inbox)
      |> Ok
    })
    |> sm.addressed(address)
    |> sm.on_event(fn(_state, value, message) {
      case message {
        Read(reply) -> {
          process.send(reply, value)
          sm.keep(value)
        }
        Stop -> sm.stop()
      }
    })
    |> sm.start
  }
  let assert Ok(first) = start(1) as "the first machine must start"
  assert read(address) == 1
  kill_and_join(first.pid)
  let assert Ok(second) = start(2) as "the replacement machine must start"
  assert read(address) == 2
  kill_and_join(second.pid)
  assert registry.stop(names) == Ok(Nil)
}

pub fn one_recipient_death_reclaims_all_its_bindings_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let first_address = registry.new_address(names)
  let assert Ok(started) = recipient(first_address, 1)
    as "the shared recipient must start"
  let addresses =
    list.map(list.repeat(Nil, 100), fn(_) {
      let address = registry.new_address(names)
      assert registry.register(address, started.data) == Ok(Nil)
      address
    })
  assert stats(names) == #(101, 101)
  kill_and_join(started.pid)

  // Lookup must refuse the dead pid even before every DOWN was processed.
  list.each(addresses, fn(address) {
    assert registry.lookup(address) == Error(Nil)
  })
  assert poll.until(within: 1000, every: 1, attempt: fn() {
      case stats(names) {
        #(0, 0) -> poll.Done(Nil)
        _ -> poll.Retry
      }
    })
    == poll.Answered(Nil)
  assert registry.stop(names) == Ok(Nil)
}

pub fn stopping_a_registry_invalidates_addresses_without_killing_recipients_test() {
  let assert Ok(names) = registry.start() as "the registry must start"
  let address = registry.new_address(names)
  let assert Ok(started) = recipient(address, 7) as "the recipient must start"
  assert registry.stop(names) == Ok(Nil)
  assert registry.lookup(address) == Error(Nil)
  assert registry.send(address, Stop) == Error(Nil)
  assert registry.register(address, started.data)
    == Error("registry unavailable")
  assert actor.call(started.data, 1000, Read) == 7
  kill_and_join(started.pid)
}

pub fn registry_death_does_not_alias_a_later_registry_test() {
  let assert Ok(first) = registry.start() as "the first registry must start"
  let old_address = registry.new_address(first)
  let inbox = process.new_subject()
  assert registry.register(old_address, inbox) == Ok(Nil)
  kill_and_join(registry.owner(first))

  let assert Ok(second) = registry.start() as "the second registry must start"
  let new_address = registry.new_address(second)
  assert registry.register(new_address, inbox) == Ok(Nil)
  assert registry.lookup(old_address) == Error(Nil)
  assert registry.lookup(new_address) == Ok(inbox)
  assert registry.stop(second) == Ok(Nil)
}

pub fn allocation_and_rebinding_do_not_grow_the_atom_table_test() {
  // Warm every path before measuring module loading separately from names.
  cycle_registry()
  let before = atom_count()
  list.each(list.repeat(Nil, 100), fn(_) { cycle_registry() })
  assert atom_count() == before
}

fn cycle_registry() -> Nil {
  let assert Ok(names) = registry.start() as "the measured registry must start"
  let address = registry.new_address(names)
  let assert Ok(first) = recipient(address, 1)
    as "the measured first recipient must start"
  assert read(address) == 1
  kill_and_join(first.pid)
  let assert Ok(second) = recipient(address, 2)
    as "the measured replacement must start"
  assert read(address) == 2
  kill_and_join(second.pid)
  assert registry.stop(names) == Ok(Nil)
}
