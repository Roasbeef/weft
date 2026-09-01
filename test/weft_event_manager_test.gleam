//// Tests for `weft/event_manager`.
////
//// The claims worth testing here are about a list, not about a value: that
//// handlers with unrelated state types really do share one, that a handler
//// leaves it without disturbing the handlers either side of it, and that a
//// successor lands in its predecessor's position rather than at one end.
//// Each of those is an ordering claim, so nearly every test asserts on a
//// *sequence* a handler wrote rather than on a single final number.
////
//// Nothing here sleeps. `sync_notify` returns only after the fan-out, so
//// every assertion that follows one reads its mailbox with a zero timeout:
//// if the effect is not there already, the claim `sync_notify` makes is
//// false, and a generous timeout would only turn that failure into a slow
//// pass or a slow failure.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/otp/static_supervisor as supervisor
import gleeunit
import weft/event_manager

pub fn main() -> Nil {
  gleeunit.main()
}

// ------------------------------------------------------------- test tools

/// Stop a manager the test started, without taking the test process with it.
///
/// `start` links the manager to its caller, so killing it unlinked is the
/// only way for a test to clean up after itself.
fn discard(pid: Pid) -> Nil {
  process.unlink(pid)
  process.kill(pid)
}

/// The bus's event type: a word for the handlers to do something with, and a
/// request that each of them report what it has accumulated.
///
/// `Flush` is how a handler's private state is observed at all. It cannot be
/// read from outside — that is the whole encoding — so the handler is asked
/// to send it somewhere the test can see.
type Event {
  Word(word: String)
  Flush
}

/// Take everything already waiting for `subject`, oldest first.
///
/// The zero timeout is the point rather than an optimisation: every caller
/// drains after a `sync_notify` has returned, so anything the handlers sent
/// is already here, and waiting would hide a broken synchronisation behind a
/// slow pass.
fn drain(subject: Subject(a)) -> List(a) {
  drain_loop(subject, [])
}

fn drain_loop(subject: Subject(a), taken: List(a)) -> List(a) {
  case process.receive(subject, 0) {
    Ok(message) -> drain_loop(subject, [message, ..taken])
    Error(Nil) -> list.reverse(taken)
  }
}

// ---------------------------------------------------------------- handlers

/// A handler whose state is an `Int`: how many words it has seen. It reports
/// the count on `Flush` and is otherwise silent.
fn counter(report: Subject(Int)) -> event_manager.Handler(Event) {
  use count, event <- event_manager.handler(0)
  case event {
    Word(_) -> Ok(count + 1)
    Flush -> {
      process.send(report, count)
      Ok(count)
    }
  }
}

/// A handler whose state is a `List(String)`: every word it has seen, newest
/// first, reported oldest-first on `Flush`.
///
/// Its state type has nothing to do with `counter`'s, which is the point of
/// putting the two in one manager.
fn recorder(report: Subject(List(String))) -> event_manager.Handler(Event) {
  use words, event <- event_manager.handler([])
  case event {
    Word(word:) -> Ok([word, ..words])
    Flush -> {
      process.send(report, list.reverse(words))
      Ok(words)
    }
  }
}

/// A stateless handler that writes `tag:word` to a shared log as it sees
/// each word.
///
/// A shared log is how the *relative* order of several handlers is observed:
/// they all write to the same subject, so the drained sequence is the order
/// the fan-out visited them in.
fn tracer(tag: String, log: Subject(String)) -> event_manager.Handler(Event) {
  use _state, event <- event_manager.handler(Nil)
  case event {
    Word(word:) -> {
      process.send(log, tag <> ":" <> word)
      Ok(Nil)
    }
    Flush -> Ok(Nil)
  }
}

/// A handler that traces like `tracer` until it is given the word "poison",
/// which it declares itself broken by.
fn brittle(log: Subject(String)) -> event_manager.Handler(Event) {
  use _state, event <- event_manager.handler(Nil)
  case event {
    Word(word: "poison") -> Error("brittle handler was given poison")
    Word(word:) -> {
      process.send(log, "brittle:" <> word)
      Ok(Nil)
    }
    Flush -> Ok(Nil)
  }
}

/// A handler written against the raw constructor: it announces itself once
/// and then retires, which is the outcome `handler`'s state-threading shape
/// cannot express.
fn once(tag: String, log: Subject(String)) -> event_manager.Handler(Event) {
  use _event <- event_manager.handler_with_outcome
  process.send(log, tag <> ":once")
  event_manager.RemoveSelf
}

/// The other half of `handler_with_outcome`: hand-written state threading,
/// where the state is a countdown and running out is a `RemoveSelf` rather
/// than a `Failed`.
fn countdown(
  remaining: Int,
  tag: String,
  log: Subject(String),
) -> event_manager.Handler(Event) {
  use _event <- event_manager.handler_with_outcome
  case remaining {
    0 -> event_manager.RemoveSelf
    n -> {
      process.send(log, tag <> ":" <> int.to_string(n))
      event_manager.Keep(countdown(n - 1, tag, log))
    }
  }
}

// ------------------------------------------- the heterogeneous handler list

pub fn handlers_with_different_state_types_share_one_list_test() -> Nil {
  // The claim gen_event exists for and Gleam's type system appears to
  // forbid: an `Int`-stated handler and a `List(String)`-stated one in the
  // same `List(Handler(Event))`, neither state type named anywhere outside
  // its own constructor.
  let counts = process.new_subject()
  let words = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(counter(counts))
    |> event_manager.add(recorder(words))
    |> event_manager.start
    as "the manager must start"

  event_manager.sync_notify(started.data, Word("alpha"), waiting: 1000)
  event_manager.sync_notify(started.data, Word("beta"), waiting: 1000)
  event_manager.sync_notify(started.data, Flush, waiting: 1000)

  assert process.receive(counts, 0) == Ok(2)
  assert process.receive(words, 0) == Ok(["alpha", "beta"])
  assert event_manager.count_handlers(started.data, waiting: 1000) == 2

  discard(started.pid)
}

pub fn handler_state_threads_across_events_test() -> Nil {
  // A small property over the counter: whatever the number of events, its
  // final state is exactly that number. Zero is in the list because an empty
  // fan-out is the case where a successor-threading bug is invisible.
  use length <- list.each([0, 1, 2, 7, 33])

  let counts = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(counter(counts))
    |> event_manager.start
    as "the manager must start"

  // Fire and forget, so this also says that `notify` loses nothing: the
  // `Flush` behind it is a `call`, and its reply cannot overtake the events
  // queued in front of it.
  list.each(list.repeat(Nil, length), fn(_ignored) {
    event_manager.notify(started.data, Word("tick"))
  })
  event_manager.sync_notify(started.data, Flush, waiting: 1000)

  assert process.receive(counts, 0) == Ok(length)

  discard(started.pid)
}

// ------------------------------------------------------- ordering and drop

pub fn handlers_run_in_add_order_test() -> Nil {
  // Order in the fan-out, and order *stability*: the second event has to
  // visit the successors in the same order the first event visited their
  // predecessors, which is the claim that a successor replaces its
  // predecessor in place rather than moving to an end of the list.
  let log = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(tracer("first", log))
    |> event_manager.add(tracer("second", log))
    |> event_manager.add(tracer("third", log))
    |> event_manager.start
    as "the manager must start"

  event_manager.sync_notify(started.data, Word("a"), waiting: 1000)
  event_manager.sync_notify(started.data, Word("b"), waiting: 1000)

  assert drain(log)
    == ["first:a", "second:a", "third:a", "first:b", "second:b", "third:b"]

  discard(started.pid)
}

pub fn a_failed_handler_is_removed_without_disturbing_siblings_test() -> Nil {
  // The isolation gen_event exists for. The brittle handler is in the middle
  // of the list on purpose: a drop that disturbed its siblings would show up
  // as a missing or reordered trace from the handler *after* it, on the very
  // event that killed it.
  let log = process.new_subject()
  let counts = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(tracer("before", log))
    |> event_manager.add(brittle(log))
    |> event_manager.add(tracer("after", log))
    |> event_manager.add(counter(counts))
    |> event_manager.start
    as "the manager must start"

  event_manager.sync_notify(started.data, Word("one"), waiting: 1000)
  event_manager.sync_notify(started.data, Word("poison"), waiting: 1000)
  event_manager.sync_notify(started.data, Word("two"), waiting: 1000)
  event_manager.sync_notify(started.data, Flush, waiting: 1000)

  // "poison" reaches both siblings and is the last event the brittle handler
  // sees; "two" reaches the siblings alone.
  assert drain(log)
    == [
      "before:one", "brittle:one", "after:one", "before:poison", "after:poison",
      "before:two", "after:two",
    ]

  // The manager itself is untouched: it still holds the other three, and the
  // counter's state threaded across the event that removed a sibling.
  assert process.receive(counts, 0) == Ok(3)
  assert event_manager.count_handlers(started.data, waiting: 1000) == 3

  discard(started.pid)
}

pub fn remove_self_removes_exactly_that_handler_test() -> Nil {
  // `RemoveSelf` is the well-behaved departure, and it has to be as
  // undisturbing as the failing one.
  let log = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(tracer("before", log))
    |> event_manager.add(once("retiring", log))
    |> event_manager.add(tracer("after", log))
    |> event_manager.start
    as "the manager must start"

  event_manager.sync_notify(started.data, Word("a"), waiting: 1000)
  event_manager.sync_notify(started.data, Word("b"), waiting: 1000)

  assert drain(log)
    == ["before:a", "retiring:once", "after:a", "before:b", "after:b"]
  assert event_manager.count_handlers(started.data, waiting: 1000) == 2

  discard(started.pid)
}

pub fn a_hand_threaded_handler_keeps_its_successor_test() -> Nil {
  // The `Keep` half of `handler_with_outcome`: state carried by hand through
  // the successor, and a departure at the end of it.
  let log = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(countdown(2, "count", log))
    |> event_manager.start
    as "the manager must start"

  event_manager.sync_notify(started.data, Word("a"), waiting: 1000)
  event_manager.sync_notify(started.data, Word("b"), waiting: 1000)
  event_manager.sync_notify(started.data, Word("c"), waiting: 1000)

  assert drain(log) == ["count:2", "count:1"]
  assert event_manager.count_handlers(started.data, waiting: 1000) == 0

  discard(started.pid)
}

// ---------------------------------------------------------- synchronisation

pub fn sync_notify_returns_only_after_every_handler_has_run_test() -> Nil {
  // The zero timeouts are the test. Nothing is in the log before the call,
  // and everything is in it the instant the call returns, with no window in
  // between for the caller to observe a half-finished fan-out.
  let log = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(tracer("first", log))
    |> event_manager.add(tracer("second", log))
    |> event_manager.start
    as "the manager must start"

  assert process.receive(log, 0) == Error(Nil)

  event_manager.sync_notify(started.data, Word("now"), waiting: 1000)

  assert drain(log) == ["first:now", "second:now"]

  discard(started.pid)
}

// ------------------------------------------------------- runtime additions

pub fn a_handler_added_at_runtime_sees_only_later_events_test() -> Nil {
  // Two claims in one sequence: the late handler misses the event that was
  // already handled, and it is appended rather than prepended, so it runs
  // last on the event it does see.
  let log = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(tracer("early", log))
    |> event_manager.start
    as "the manager must start"

  event_manager.sync_notify(started.data, Word("before"), waiting: 1000)

  // `add_handler` is fire-and-forget, and needs no synchronisation of its
  // own: messages from one process to one process keep their order, so the
  // event sent next cannot be handled before the handler is in the list.
  event_manager.add_handler(started.data, tracer("late", log))
  event_manager.sync_notify(started.data, Word("after"), waiting: 1000)

  assert drain(log) == ["early:before", "early:after", "late:after"]
  assert event_manager.count_handlers(started.data, waiting: 1000) == 2

  discard(started.pid)
}

pub fn count_handlers_tracks_additions_and_departures_test() -> Nil {
  // The count is the only external view of the list, so it is worth walking
  // it through every way the list can change: built, added to, retired from,
  // and failed out of.
  let log = process.new_subject()

  let assert Ok(started) =
    event_manager.new()
    |> event_manager.add(tracer("keep", log))
    |> event_manager.add(once("retiring", log))
    |> event_manager.add(brittle(log))
    |> event_manager.start
    as "the manager must start"

  assert event_manager.count_handlers(started.data, waiting: 1000) == 3

  event_manager.add_handler(started.data, tracer("late", log))
  assert event_manager.count_handlers(started.data, waiting: 1000) == 4

  // The retiring handler goes on this event.
  event_manager.sync_notify(started.data, Word("hello"), waiting: 1000)
  assert event_manager.count_handlers(started.data, waiting: 1000) == 3

  // The brittle one goes on this one.
  event_manager.sync_notify(started.data, Word("poison"), waiting: 1000)
  assert event_manager.count_handlers(started.data, waiting: 1000) == 2

  discard(started.pid)
}

pub fn an_empty_manager_handles_events_test() -> Nil {
  // A bus started before its consumers exist is the ordinary supervised
  // shape, so an empty fan-out has to be a no-op rather than an error.
  let assert Ok(started) = event_manager.new() |> event_manager.start
    as "the manager must start with no handlers"

  event_manager.notify(started.data, Word("nobody is listening"))
  event_manager.sync_notify(started.data, Flush, waiting: 1000)

  assert event_manager.count_handlers(started.data, waiting: 1000) == 0

  discard(started.pid)
}

// ---------------------------------------------------------- naming and OTP

pub fn a_named_manager_runs_under_a_supervisor_test() -> Nil {
  // The manager is a weft actor, so it inherits naming and supervision
  // whole. This is the test that says the inheritance is real rather than
  // claimed: the bus is reached only by its name, through a supervisor that
  // never saw a subject.
  let log = process.new_subject()
  let name = process.new_name("weft_event_manager_supervised")

  let child =
    event_manager.new()
    |> event_manager.add(tracer("supervised", log))
    |> event_manager.named(name)
    |> event_manager.supervised

  let assert Ok(started) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(child)
    |> supervisor.start
    as "the supervisor must start its manager child"

  let bus = process.named_subject(name)
  event_manager.sync_notify(bus, Word("hello"), waiting: 1000)

  assert process.receive(log, 0) == Ok("supervised:hello")

  discard(started.pid)
}
