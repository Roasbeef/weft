//// Tests for managed tasks: the drain-proof half of the run engine.
////
//// Each test is named for the invariant it defends. The recurring cast is
//// an *obedient owner* — a process that stands for a client library's
//// socket-holding subtree, exits normally when its cancel capability is
//// invoked, and can be killed from outside to model a lost proof — and a
//// *deaf owner*, which ignores its cancel entirely so the grace path has
//// something to time out on.
////
//// The tests that watch a scope's exit reason are the composition story:
//// an outer witness learns the run's drain verdict from the one channel
//// monitors already provide, so the verdict must be exactly right in both
//// directions — normal when every proof landed, abnormal when any did not.

import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/otp/system
import weft.{
  Abandoned, AllDelivered, CancellationUnconfirmed, Completed, DrainProofLost,
  NeverStarted, NotYet, PulledOutcome,
}

// --- The owners the tests run against ----------------------------------------

/// What a test tells an owner to do.
type OwnerCommand {
  /// Finish cleanly: the subtree has drained.
  DrainCleanly
}

/// Spawn a process that stays alive until told to drain, then exits
/// normally. Returns the pid and the subject its cancel capability speaks
/// on; the subject is created inside the owner because a subject receives
/// only in the process that made it.
fn obedient_owner() -> #(Pid, Subject(OwnerCommand)) {
  let handoff = process.new_subject()

  let pid =
    process.spawn_unlinked(fn() {
      let commands = process.new_subject()
      process.send(handoff, commands)

      let DrainCleanly = process.receive_forever(commands)
      Nil
    })

  let assert Ok(commands) = process.receive(handoff, 2000)
    as "a freshly spawned owner hands its command subject over immediately"
  #(pid, commands)
}

/// A cancel capability that asks an obedient owner to drain. Safe to call
/// any number of times: extra commands land in a mailbox nobody reads twice.
fn drain_on_cancel(commands: Subject(OwnerCommand)) -> fn() -> Nil {
  fn() { process.send(commands, DrainCleanly) }
}

/// A cancel capability that does nothing, for owners that must outlive the
/// grace.
fn deaf_cancel() -> fn() -> Nil {
  fn() { Nil }
}

/// Pull until the run has delivered everything, gathering outcomes.
fn pull_all(
  detached: weft.Detached(a, e),
  gathered: List(weft.Outcome(a, e)),
) -> List(weft.Outcome(a, e)) {
  case weft.pull(detached, within: 5000) {
    PulledOutcome(outcome) -> pull_all(detached, [outcome, ..gathered])
    AllDelivered -> list.reverse(gathered)
    NotYet -> pull_all(detached, gathered)
    weft.RunLost(..) -> list.reverse(gathered)
  }
}

/// Start watching a pid's exit. Taken out *before* the assertions that let
/// the process finish, so the `DOWN` carries the true exit reason rather
/// than the `noproc` a late monitor reads off a corpse.
fn watch_exit(pid: Pid) -> process.Selector(process.ExitReason) {
  let watch = process.monitor(pid)
  process.new_selector()
  |> process.select_specific_monitor(watch, fn(down) {
    case down {
      process.ProcessDown(reason:, ..) -> reason
      process.PortDown(reason:, ..) -> reason
    }
  })
}

/// Collect the reason from a watch taken out earlier.
fn await_exit(
  downs: process.Selector(process.ExitReason),
) -> process.ExitReason {
  let assert Ok(reason) = process.selector_receive(downs, 5000)
    as "the watched process exits within the test budget"
  reason
}

/// Does this abnormal exit reason carry the given atom?
fn abnormal_named(reason: process.ExitReason, name: String) -> Bool {
  case reason {
    process.Abnormal(reason: carried) ->
      decode.run(carried, atom.decoder()) == Ok(atom.create(name))
    process.Normal -> False
    process.Killed -> False
  }
}

// --- Completion is gated on the owner ----------------------------------------

pub fn a_managed_outcome_waits_for_its_owners_exit_test() -> Nil {
  let #(owner, commands) = obedient_owner()

  let detached =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: drain_on_cancel(commands), begin: fn() {
        Ok("answered")
      }),
    ])
    |> weft.start_detached

  // The worker returns almost immediately, but the owner is alive, so the
  // outcome is withheld: this pull must time out.
  assert weft.pull(detached, within: 200) == NotYet

  // The owner draining is what releases the outcome, unchanged.
  process.send(commands, DrainCleanly)
  assert weft.pull(detached, within: 5000)
    == PulledOutcome(Completed(index: 0, value: "answered"))

  assert weft.pull(detached, within: 5000) == AllDelivered
}

pub fn an_abnormal_owner_exit_is_a_lost_proof_not_a_crash_test() -> Nil {
  let #(owner, _commands) = obedient_owner()

  let detached =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: deaf_cancel(), begin: fn() { Ok(1) }),
    ])
    |> weft.start_detached
  let scope_exit = watch_exit(weft.scope_pid(detached))

  // The worker succeeds, then the owner is destroyed from outside: the
  // value is not deliverable, because nobody can say the work stopped.
  process.kill(owner)
  let assert PulledOutcome(DrainProofLost(index: 0, reason: process.Killed)) =
    weft.pull(detached, within: 5000)
    as "a killed transitive owner settles its task as a lost proof"
  assert weft.pull(detached, within: 5000) == AllDelivered

  // And the scope repeats the verdict outward as its exit reason.
  assert abnormal_named(await_exit(scope_exit), "weft_drain_proof_lost")
}

pub fn an_owner_dead_before_the_run_never_lets_begin_run_test() -> Nil {
  let #(owner, commands) = obedient_owner()
  process.send(commands, DrainCleanly)

  // The owner is gone before the run exists; proof was never on file.
  let ran = process.new_subject()
  let outcomes =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: deaf_cancel(), begin: fn() {
        process.send(ran, True)
        Ok(1)
      }),
    ])
    |> weft.start

  let assert [DrainProofLost(index: 0, ..)] = outcomes
    as "an already-dead owner is a lost proof, not a fast drain"
  assert process.receive(ran, 200) == Error(Nil)
}

pub fn a_leaf_owner_dead_before_the_run_is_still_accounted_for_test() -> Nil {
  let #(owner, commands) = obedient_owner()
  process.send(commands, DrainCleanly)

  // The leaf exemption covers an ordinary crash of work that ran. An owner
  // that was a corpse before the run existed proved nothing, and its task
  // — whose worker is never admitted — must still appear in the account
  // rather than vanish from it.
  let ran = process.new_subject()
  let outcomes =
    weft.new_prepared([
      weft.prepared_leaf(owner:, cancel: deaf_cancel(), begin: fn() {
        process.send(ran, True)
        Ok(1)
      }),
      weft.task(fn() { Ok(2) }),
    ])
    |> weft.start

  let assert [DrainProofLost(index: 0, ..), Completed(index: 1, value: 2)] =
    outcomes
    as "a dead-on-arrival leaf is a lost proof, and the account stays total"
  assert process.receive(ran, 200) == Error(Nil)
}

pub fn a_leaf_owners_crash_is_not_a_lost_proof_test() -> Nil {
  let #(owner, _commands) = obedient_owner()

  let detached =
    weft.new_prepared([
      weft.prepared_leaf(owner:, cancel: deaf_cancel(), begin: fn() { Ok(7) }),
    ])
    |> weft.start_detached
  let scope_exit = watch_exit(weft.scope_pid(detached))

  // A leaf owner declared it owns nothing further, so any exit completes
  // it and the worker's own answer stands.
  process.kill(owner)
  assert weft.pull(detached, within: 5000)
    == PulledOutcome(Completed(index: 0, value: 7))
  assert weft.pull(detached, within: 5000) == AllDelivered

  assert await_exit(scope_exit) == process.Normal
}

// --- The slot is held until the drain ----------------------------------------

pub fn a_managed_task_holds_its_slot_until_its_owner_exits_test() -> Nil {
  let #(owner, commands) = obedient_owner()
  let second_started = process.new_subject()

  let detached =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: drain_on_cancel(commands), begin: fn() {
        Ok("managed")
      }),
      weft.task(fn() {
        process.send(second_started, True)
        Ok("plain")
      }),
    ])
    |> weft.limit(1)
    |> weft.start_detached

  // The managed worker has long since returned, but its owner is alive, so
  // its slot — the only slot — is still occupied and the second task must
  // not have started.
  assert weft.pull(detached, within: 200) == NotYet
  assert process.receive(second_started, 200) == Error(Nil)

  // Draining the owner releases the outcome, the slot with it, and only
  // then the second task.
  process.send(commands, DrainCleanly)
  assert weft.pull(detached, within: 5000)
    == PulledOutcome(Completed(index: 0, value: "managed"))
  assert weft.pull(detached, within: 5000)
    == PulledOutcome(Completed(index: 1, value: "plain"))
  assert weft.pull(detached, within: 5000) == AllDelivered
}

// --- Cancellation asks owners, and the grace bounds the wait ------------------

pub fn cancellation_reaches_the_owner_and_waits_for_its_exit_test() -> Nil {
  let #(owner, commands) = obedient_owner()

  let detached =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: drain_on_cancel(commands), begin: fn() {
        process.sleep(60_000)
        Ok(0)
      }),
    ])
    |> weft.start_detached
  let scope_exit = watch_exit(weft.scope_pid(detached))

  // Cancelling kills the worker and *asks* the owner, whose obedient exit
  // proves the drain: the account still arrives, and the scope's exit is
  // clean.
  weft.cancel_detached(detached)
  assert weft.pull(detached, within: 5000) == PulledOutcome(Abandoned(index: 0))
  assert weft.pull(detached, within: 5000) == AllDelivered

  assert await_exit(scope_exit) == process.Normal
}

pub fn an_owners_exit_dismisses_a_cancel_helper_that_is_still_running_test() -> Nil {
  let #(owner, commands) = obedient_owner()

  // The cancel closure asks the owner to drain and then never returns. The
  // owner's exit is the fact the run was waiting on; the helper that asked
  // for it must not hold the scope open once that fact is in.
  let stuck_cancel = fn() {
    process.send(commands, DrainCleanly)
    process.sleep_forever()
  }

  let finished = process.new_subject()
  process.spawn_unlinked(fn() {
    let outcomes =
      weft.new_prepared([
        weft.prepared_task(owner:, cancel: stuck_cancel, begin: fn() {
          process.sleep_forever()
          Ok(Nil)
        }),
      ])
      |> weft.deadline(50)
      |> weft.start
    process.send(finished, outcomes)
  })

  let assert Ok([Abandoned(index: 0)]) = process.receive(finished, 3000)
    as "the run ends once the owner drains, however long its canceller lingers"
  Nil
}

pub fn a_deaf_owner_settles_as_unconfirmed_once_the_grace_expires_test() -> Nil {
  let #(owner, _commands) = obedient_owner()

  let detached =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: deaf_cancel(), begin: fn() {
        process.sleep(60_000)
        Ok(0)
      }),
    ])
    |> weft.cancel_grace(300)
    |> weft.start_detached
  let scope_exit = watch_exit(weft.scope_pid(detached))

  // The owner ignores its cancel, so nothing can prove the work stopped;
  // the grace turns that silence into a bounded, honest answer.
  weft.cancel_detached(detached)
  assert weft.pull(detached, within: 5000)
    == PulledOutcome(CancellationUnconfirmed(index: 0))
  assert weft.pull(detached, within: 5000) == AllDelivered

  // The scope can no longer vouch for the subtree, and says so on the way
  // out. The owner is still alive — it was asked, never killed.
  assert abnormal_named(await_exit(scope_exit), "weft_drain_unconfirmed")
  assert process.is_alive(owner)

  process.kill(owner)
}

pub fn a_cancelled_run_still_accounts_for_unstarted_managed_tasks_test() -> Nil {
  let #(first_owner, first_commands) = obedient_owner()
  let #(second_owner, second_commands) = obedient_owner()

  let detached =
    weft.new_prepared([
      weft.prepared_task(
        owner: first_owner,
        cancel: drain_on_cancel(first_commands),
        begin: fn() {
          process.sleep(60_000)
          Ok(0)
        },
      ),
      weft.prepared_task(
        owner: second_owner,
        cancel: drain_on_cancel(second_commands),
        begin: fn() {
          process.sleep(60_000)
          Ok(0)
        },
      ),
    ])
    |> weft.limit(1)
    |> weft.start_detached
  let scope_exit = watch_exit(weft.scope_pid(detached))

  // The second task never got a slot, but its owner exists and is owed a
  // drain: cancellation must reach both owners, and the account must carry
  // one entry per task all the same.
  weft.cancel_detached(detached)
  let outcomes = pull_all(detached, [])
  assert list.contains(outcomes, Abandoned(index: 0))
  assert list.contains(outcomes, NeverStarted(index: 1))

  assert await_exit(scope_exit) == process.Normal
}

// --- The scope composes ------------------------------------------------------

pub fn a_dead_holder_still_gets_its_owners_drained_test() -> Nil {
  let #(owner, commands) = obedient_owner()
  let scopes = process.new_subject()

  // The holder starts a detached run and then sits there; the test destroys
  // it mid-run. The scope survives the exit signal, asks the owner to stop,
  // and exits — normally, because the obedient owner's exit proves the
  // drain. Nothing leaks from a run whose holder is gone.
  let holder =
    process.spawn_unlinked(fn() {
      let detached =
        weft.new_prepared([
          weft.prepared_task(
            owner:,
            cancel: drain_on_cancel(commands),
            begin: fn() {
              process.sleep(60_000)
              Ok(0)
            },
          ),
        ])
        |> weft.start_detached
      process.send(scopes, weft.scope_pid(detached))
      process.sleep_forever()
    })

  let assert Ok(scope) = process.receive(scopes, 2000)
    as "the holder hands the scope pid over before it is killed"
  let scope_exit = watch_exit(scope)
  process.kill(holder)

  assert await_exit(scope_exit) == process.Normal
  assert !process.is_alive(owner)
}

pub fn a_nested_scope_is_a_publishable_owner_test() -> Nil {
  // The inner run is real managed work; its detached scope is then handed
  // to the outer run as the published owner. No translation code exists on
  // either side: the outer proof *is* the inner scope's exit verdict.
  let #(owner, commands) = obedient_owner()
  let inner =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: drain_on_cancel(commands), begin: fn() {
        Ok("inner")
      }),
    ])
    |> weft.start_detached

  let outer =
    weft.new_prepared([
      weft.prepared_task(
        owner: weft.scope_pid(inner),
        cancel: fn() { weft.cancel_detached(inner) },
        begin: fn() { Ok("outer") },
      ),
    ])
    |> weft.start_detached

  // The outer outcome is withheld while the inner run is still draining...
  assert weft.pull(outer, within: 200) == NotYet

  // ...and releases exactly when the inner scope's normal exit proves the
  // whole inner run, owner included, is gone.
  process.send(commands, DrainCleanly)
  assert pull_all(inner, []) == [Completed(index: 0, value: "inner")]
  assert weft.pull(outer, within: 5000)
    == PulledOutcome(Completed(index: 0, value: "outer"))
  assert weft.pull(outer, within: 5000) == AllDelivered
}

pub fn a_lost_inner_proof_poisons_the_outer_scope_test() -> Nil {
  let #(owner, _commands) = obedient_owner()
  let inner =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: deaf_cancel(), begin: fn() { Ok(1) }),
    ])
    |> weft.start_detached

  let outer =
    weft.new_prepared([
      weft.prepared_task(
        owner: weft.scope_pid(inner),
        cancel: fn() { weft.cancel_detached(inner) },
        begin: fn() { Ok(2) },
      ),
    ])
    |> weft.start_detached

  // Killing the innermost owner loses the inner proof. The inner account
  // still has to be drained — a scope holds undelivered outcomes — and only
  // then does the inner scope exit abnormally, which is, verbatim, the
  // outer task's lost proof.
  process.kill(owner)
  let assert [DrainProofLost(index: 0, ..)] = pull_all(inner, [])
    as "the inner run accounts its own lost proof first"
  let assert PulledOutcome(DrainProofLost(index: 0, ..)) =
    weft.pull(outer, within: 5000)
    as "the inner scope's abnormal exit propagates as the outer lost proof"
  assert weft.pull(outer, within: 5000) == AllDelivered
}

// --- Push delivery and the system plane ---------------------------------------

pub fn a_relayed_run_pushes_outcomes_and_then_one_terminal_test() -> Nil {
  let sink = process.new_subject()

  let _relay =
    weft.new([fn() { Ok(1) }, fn() { Ok(2) }])
    |> weft.start_relayed(to: sink)

  let assert Ok(PulledOutcome(Completed(..))) = process.receive(sink, 5000)
    as "the relay forwards the first outcome as a message"
  let assert Ok(PulledOutcome(Completed(..))) = process.receive(sink, 5000)
    as "the relay forwards the second outcome as a message"
  assert process.receive(sink, 5000) == Ok(AllDelivered)
}

pub fn a_scope_answers_the_system_plane_test() -> Nil {
  let #(owner, commands) = obedient_owner()

  let detached =
    weft.new_prepared([
      weft.prepared_task(owner:, cancel: drain_on_cancel(commands), begin: fn() {
        Ok(1)
      }),
    ])
    |> weft.start_detached
  let scope = weft.scope_pid(detached)

  // `sys:get_state/1` blocks its caller until the process answers, so this
  // returning at all is the property: the scope is visible to the tools.
  let _state = system.get_state(scope)

  // A suspended scope serves nothing but the system plane; resuming it
  // lets the run finish as if nothing happened.
  system.suspend(scope)
  process.send(commands, DrainCleanly)
  system.resume(scope)

  assert weft.pull(detached, within: 5000)
    == PulledOutcome(Completed(index: 0, value: 1))
  assert weft.pull(detached, within: 5000) == AllDelivered
}

// --- Owners discovered while the task runs -----------------------------------

pub fn a_managed_task_adopts_owners_as_it_runs_test() -> Nil {
  let #(owner, commands) = obedient_owner()

  let detached =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        let assert weft.Adopted =
          weft.adopt(ledger, owner:, cancel: drain_on_cancel(commands))
          as "a live run adopts an owner published mid-flight"
        Ok("answered")
      }),
    ])
    |> weft.start_detached
  let scope_exit = watch_exit(weft.scope_pid(detached))

  // The worker has returned, but the owner it published is still alive, so
  // the outcome is withheld exactly as it would be for a prepared owner.
  assert weft.pull(detached, within: 200) == NotYet

  process.send(commands, DrainCleanly)
  assert weft.pull(detached, within: 5000)
    == PulledOutcome(Completed(index: 0, value: "answered"))
  assert weft.pull(detached, within: 5000) == AllDelivered
  assert await_exit(scope_exit) == process.Normal
}

pub fn a_task_with_two_owners_is_drained_only_when_both_are_test() -> Nil {
  let #(first, first_commands) = obedient_owner()
  let #(second, second_commands) = obedient_owner()

  // The worker adopts asynchronously; the test must not drain an owner
  // the scope has not yet monitored, or the drain reads as an owner that
  // was dead on adoption.
  let adopted = process.new_subject()
  let detached =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        let assert weft.Adopted =
          weft.adopt(ledger, owner: first, cancel: deaf_cancel())
          as "the first owner is adopted"
        let assert weft.Adopted =
          weft.adopt(ledger, owner: second, cancel: deaf_cancel())
          as "the second owner is adopted"
        process.send(adopted, Nil)
        Ok(2)
      }),
    ])
    |> weft.start_detached
  let assert Ok(Nil) = process.receive(adopted, 2000)
    as "both owners are adopted before the test drains either"

  process.send(first_commands, DrainCleanly)
  assert weft.pull(detached, within: 200) == NotYet

  process.send(second_commands, DrainCleanly)
  assert weft.pull(detached, within: 5000)
    == PulledOutcome(Completed(index: 0, value: 2))
  assert weft.pull(detached, within: 5000) == AllDelivered
}

pub fn one_lost_owner_seals_the_task_exactly_once_test() -> Nil {
  let #(first, _first_commands) = obedient_owner()
  let #(second, second_commands) = obedient_owner()

  let adopted = process.new_subject()
  let detached =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        let assert weft.Adopted =
          weft.adopt(ledger, owner: first, cancel: deaf_cancel())
          as "the first owner is adopted"
        let assert weft.Adopted =
          weft.adopt(ledger, owner: second, cancel: deaf_cancel())
          as "the second owner is adopted"
        process.send(adopted, Nil)
        Ok(3)
      }),
    ])
    |> weft.start_detached
  let scope_exit = watch_exit(weft.scope_pid(detached))
  let assert Ok(Nil) = process.receive(adopted, 2000)
    as "both owners are adopted before the test kills one"

  // Losing one owner loses the task at once; the other owner's later clean
  // exit must not write a second entry for the same task.
  process.kill(first)
  let assert PulledOutcome(DrainProofLost(index: 0, ..)) =
    weft.pull(detached, within: 5000)
    as "a lost owner settles the task as a lost proof"

  process.send(second_commands, DrainCleanly)
  assert weft.pull(detached, within: 5000) == AllDelivered
  assert abnormal_named(await_exit(scope_exit), "weft_drain_proof_lost")
}

pub fn adoption_after_cancellation_is_refused_but_still_drained_test() -> Nil {
  let #(first, first_commands) = obedient_owner()
  let #(second, second_commands) = obedient_owner()

  // The ledger travels: the worker hands it to this process and parks, so
  // an adoption can arrive from outside the worker after cancellation has
  // already killed the worker itself.
  let handoff = process.new_subject()
  let detached =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        process.send(handoff, ledger)
        process.sleep_forever()
        Ok(Nil)
      }),
    ])
    |> weft.start_detached
  let assert Ok(ledger) = process.receive(handoff, 2000)
    as "the worker hands its ledger over"

  assert weft.adopt(
      ledger,
      owner: first,
      cancel: drain_on_cancel(first_commands),
    )
    == weft.Adopted

  // Cancellation and the late publication go to the same inbox from the
  // same process, so the scope sees them in this order. The watch on the
  // late owner is taken first, so its true exit reason is read rather
  // than the `noproc` a late monitor finds.
  let second_exit = watch_exit(second)
  weft.cancel_detached(detached)
  assert weft.adopt(
      ledger,
      owner: second,
      cancel: drain_on_cancel(second_commands),
    )
    == weft.Refused

  // Refused is not abandoned: the scope asked the late owner to stop and
  // will wait for it exactly as for the adopted one.
  assert await_exit(second_exit) == process.Normal
  assert weft.pull(detached, within: 5000) == PulledOutcome(Abandoned(index: 0))
  assert weft.pull(detached, within: 5000) == AllDelivered
}

// --- Witnessed runs and consumer death -----------------------------------------

pub fn a_witnessed_run_reports_only_through_its_exit_test() -> Nil {
  let #(owner, commands) = obedient_owner()

  let witnessed =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        let assert weft.Adopted =
          weft.adopt(ledger, owner:, cancel: drain_on_cancel(commands))
          as "the witnessed task adopts its owner"
        Ok(Nil)
      }),
    ])
    |> weft.start_witnessed
  let scope = weft.witness_pid(witnessed)
  let scope_exit = watch_exit(scope)

  // Nobody pulls, and nothing is owed: the scope stays alive for exactly as
  // long as the owner does, then exits with the verdict.
  process.sleep(100)
  assert process.is_alive(scope)

  process.send(commands, DrainCleanly)
  assert await_exit(scope_exit) == process.Normal
}

pub fn cancelling_a_witnessed_run_asks_its_owners_and_drains_test() -> Nil {
  let #(owner, commands) = obedient_owner()

  let witnessed =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        let assert weft.Adopted =
          weft.adopt(ledger, owner:, cancel: drain_on_cancel(commands))
          as "the witnessed task adopts its owner"
        process.sleep_forever()
        Ok(Nil)
      }),
    ])
    |> weft.start_witnessed
  let scope_exit = watch_exit(weft.witness_pid(witnessed))

  // The worker is parked forever; only cancellation can end this run, and
  // it ends normally because the owner obeys its ask. The second cancel
  // is the idempotence check.
  weft.cancel_witnessed(witnessed)
  weft.cancel_witnessed(witnessed)
  assert await_exit(scope_exit) == process.Normal
}

pub fn a_witnessed_run_carries_a_lost_proof_in_its_exit_test() -> Nil {
  let #(owner, _commands) = obedient_owner()

  let witnessed =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        let assert weft.Adopted =
          weft.adopt(ledger, owner:, cancel: deaf_cancel())
          as "the witnessed task adopts its owner"
        Ok(Nil)
      }),
    ])
    |> weft.start_witnessed
  let scope_exit = watch_exit(weft.witness_pid(witnessed))

  process.kill(owner)
  assert abnormal_named(await_exit(scope_exit), "weft_drain_proof_lost")
}

pub fn a_watched_consumers_death_cancels_the_run_test() -> Nil {
  let consumer = process.spawn_unlinked(fn() { process.sleep_forever() })

  let detached =
    weft.new([
      fn() {
        process.sleep_forever()
        Ok(Nil)
      },
    ])
    |> weft.cancel_when_exits(consumer)
    |> weft.start_detached

  assert weft.pull(detached, within: 200) == NotYet
  process.kill(consumer)
  assert weft.pull(detached, within: 5000) == PulledOutcome(Abandoned(index: 0))
  assert weft.pull(detached, within: 5000) == AllDelivered
}

pub fn a_pid_both_watched_and_adopted_answers_both_watches_test() -> Nil {
  let #(worker, commands) = obedient_owner()

  // The parked-worker shape a custodian needs: the same pid is an owner
  // (so the task waits for it and cancellation asks it to stop) and a
  // watched exit (so its death, however it dies, fans cancellation out to
  // the owners adopted beside it). Two monitors, two `DOWN`s, and each
  // must do exactly its own job.
  let adopted = process.new_subject()
  let detached =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        let assert weft.Adopted =
          weft.adopt_leaf(ledger, owner: worker, cancel: deaf_cancel())
          as "the worker is adopted as a leaf"
        process.send(adopted, Nil)
        process.sleep_forever()
        Ok(Nil)
      }),
    ])
    |> weft.cancel_when_exits(worker)
    |> weft.start_detached
  let scope_exit = watch_exit(weft.scope_pid(detached))
  let assert Ok(Nil) = process.receive(adopted, 2000)
    as "the worker is adopted before it is drained"

  process.send(commands, DrainCleanly)
  assert weft.pull(detached, within: 5000) == PulledOutcome(Abandoned(index: 0))
  assert weft.pull(detached, within: 5000) == AllDelivered
  assert await_exit(scope_exit) == process.Normal
}

// --- Owners published beneath a parent ---------------------------------------

/// The ledger and a parked worker, for tests that adopt from outside.
fn parked_run() -> #(weft.Detached(Nil, Nil), weft.Ledger) {
  let handoff = process.new_subject()
  let detached =
    weft.new_prepared([
      weft.managed(fn(ledger) {
        process.send(handoff, ledger)
        process.sleep_forever()
        Ok(Nil)
      }),
    ])
    |> weft.start_detached
  let assert Ok(ledger) = process.receive(handoff, 2000)
    as "the worker hands its ledger over"
  #(detached, ledger)
}

pub fn a_child_is_asked_only_after_its_parent_exits_test() -> Nil {
  let #(parent, parent_commands) = obedient_owner()
  let #(child, child_commands) = obedient_owner()
  let asked = process.new_subject()
  let #(detached, ledger) = parked_run()

  // The parent ignores its ask, so it stays alive across cancellation;
  // the child's cancel records that it ran and then drains the child.
  assert weft.adopt_leaf(ledger, owner: parent, cancel: deaf_cancel())
    == weft.Adopted
  assert weft.adopt_under(ledger, parent:, owner: child, cancel: fn() {
      process.send(asked, Nil)
      process.send(child_commands, DrainCleanly)
    })
    == weft.Adopted

  weft.cancel_detached(detached)
  assert process.receive(asked, 300) == Error(Nil)
  assert process.is_alive(child)

  // Only the parent's own exit reaches the child.
  process.send(parent_commands, DrainCleanly)
  assert process.receive(asked, 2000) == Ok(Nil)
  assert weft.pull(detached, within: 5000) == PulledOutcome(Abandoned(index: 0))
  assert weft.pull(detached, within: 5000) == AllDelivered
}

pub fn a_parents_exit_asks_its_children_without_any_cancellation_test() -> Nil {
  let #(parent, parent_commands) = obedient_owner()
  let #(child, child_commands) = obedient_owner()
  let #(detached, ledger) = parked_run()

  assert weft.adopt_leaf(ledger, owner: parent, cancel: deaf_cancel())
    == weft.Adopted
  assert weft.adopt_under(
      ledger,
      parent:,
      owner: child,
      cancel: drain_on_cancel(child_commands),
    )
    == weft.Adopted

  // Nobody cancels the run. The parent finishing is what asks the child,
  // and the child obeying is what lets the task settle once the worker
  // does.
  process.send(parent_commands, DrainCleanly)
  let child_exit = watch_exit(child)
  assert await_exit(child_exit) == process.Normal
  weft.cancel_detached(detached)
  assert weft.pull(detached, within: 5000) == PulledOutcome(Abandoned(index: 0))
  assert weft.pull(detached, within: 5000) == AllDelivered
}

pub fn adopting_under_an_exited_parent_is_refused_and_asked_test() -> Nil {
  let #(parent, parent_commands) = obedient_owner()
  let #(child, child_commands) = obedient_owner()
  let #(detached, ledger) = parked_run()

  assert weft.adopt_leaf(ledger, owner: parent, cancel: deaf_cancel())
    == weft.Adopted
  let parent_exit = watch_exit(parent)
  process.send(parent_commands, DrainCleanly)
  assert await_exit(parent_exit) == process.Normal

  // The scope has seen the parent go by the time this reply comes back,
  // because the parent's DOWN precedes the publication in its mailbox
  // only if it arrived first — so wait for the scope to have resolved
  // it by asking through the same inbox after the exit was observed.
  let child_exit = watch_exit(child)
  assert weft.adopt_under(
      ledger,
      parent:,
      owner: child,
      cancel: drain_on_cancel(child_commands),
    )
    == weft.Refused
  assert await_exit(child_exit) == process.Normal
  weft.cancel_detached(detached)
  assert weft.pull(detached, within: 5000) == PulledOutcome(Abandoned(index: 0))
  assert weft.pull(detached, within: 5000) == AllDelivered
}
