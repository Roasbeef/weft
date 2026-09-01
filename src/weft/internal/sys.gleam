//// The OTP plumbing every weft receive loop shares: the system-message
//// plane, hibernation, and the one logging edge.
////
//// A process that wants to be visible to `sys:get_state/1`, `observer` and
//// a supervisor's shutdown is not free to just receive its own messages. It
//// has to recognise `{system, From, Request}` in its mailbox, answer on the
//// reply channel the caller is blocked on, and — for `suspend` — stop
//// serving everything else until a matching `resume` arrives. Get any part
//// of that wrong and the failure is not a crash but a hang somewhere else:
//// `sys:suspend/1` waits forever on a reply that never comes, and the
//// debugging tool you reached for is now the thing that is stuck.
////
//// That protocol is identical for the actor, the state machine and the
//// event manager, and it is exactly the kind of code that rots when it is
//// copied: three loops, three chances to answer `get_status` with a shape
//// the observer cannot render. So it is written once, here, and each loop
//// contributes only the two things that are genuinely its own — where the
//// selector arm is merged, and what it does with a `Plane` whose mode has
//// just changed.
////
//// The dynamic edge is closed on the Erlang side. `convert_system_message`
//// answers for every term, including a malformed `system` message, so a
//// loop sees `Unimplemented` where it might otherwise have died of a
//// function clause inside a foreign module. Nothing here asserts on a
//// decode.
////
//// Two neighbours ride along because they are the same kind of thing —
//// OTP process plumbing that a pure module must not reach for, and that
//// would otherwise scatter `@external` across the library. `hibernate`
//// belongs to the same "how a loop waits" question as `suspend` does, and
//// `warn` is the single place weft speaks to the OTP logger.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/charlist.{type Charlist}
import gleam/erlang/process.{type Pid, type Selector}
import gleam/otp/system.{
  type Mode, type SystemMessage, GetState, GetStatus, Resume, Running,
  StatusInfo, Suspend, Suspended,
}

/// Something that arrived on the `system` tag.
///
/// A `system`-tagged message is not necessarily one weft knows how to
/// answer: OTP defines `replace_state`, `change_code`, `terminate` and the
/// `debug` family too, and a stray tuple can always be sent by hand. Both
/// possibilities are variants rather than one being a crash, because the
/// decision about an unimplemented request belongs to the loop — an actor
/// logs and carries on — and not to the decoder.
pub type Incoming {
  /// A request this library implements, paired with the continuation that
  /// replies to the blocked caller. The loop must pass it to `handle`, or
  /// the caller waits forever.
  Request(message: SystemMessage)

  /// A `system`-tagged message with no implementation here. Carries the raw
  /// request so a loop can log what it saw.
  Unimplemented(request: Dynamic)
}

/// A loop's debug plane: everything `sys` needs to know about it, and the
/// suspend/resume mode that decides what the loop is allowed to serve.
///
/// It is opaque because `mode` is not a field a loop may set directly. The
/// mode changes only as the acknowledged half of a handshake, in `handle`,
/// which is what keeps "the caller has been told" and "the loop has stopped
/// serving messages" from drifting apart.
pub opaque type Plane {
  Plane(module: Atom, parent: Pid, mode: Mode, debug_state: system.DebugState)
}

/// Create a plane for a loop running `module` on behalf of `parent`.
///
/// `module` is the behaviour name the observer displays and `sys:get_status`
/// reports, so it should name the weft module running the loop rather than
/// the user's callback module: `weft@actor`, `weft@state_machine`.
///
/// `parent` is the process that started this one. It is reported to `sys`
/// and, for a loop that traps exits, it is the process whose exit means
/// this loop should shut down too.
///
/// ## Examples
///
/// ```gleam
/// let plane = sys.new(module: "weft@actor", parent: parent_pid)
/// // -> a running plane; the loop serves everything
/// ```
pub fn new(module module: String, parent parent: Pid) -> Plane {
  Plane(
    module: atom.create(module),
    parent:,
    mode: Running,
    debug_state: system.debug_state([]),
  )
}

/// The process that started the loop this plane belongs to.
///
/// ## Examples
///
/// ```gleam
/// case exit.pid == sys.parent(plane) {
///   True -> shut_down(exit.reason)
///   False -> keep_going()
/// }
/// ```
pub fn parent(plane: Plane) -> Pid {
  plane.parent
}

/// The plane's current mode.
///
/// ## Examples
///
/// ```gleam
/// assert sys.mode(sys.new(module: "weft@actor", parent: pid)) == system.Running
/// ```
pub fn mode(plane: Plane) -> Mode {
  plane.mode
}

/// Whether the loop is suspended and must serve system messages only.
///
/// A suspended loop may not handle mailbox messages, may not handle work it
/// has queued for itself, and may not time out: `sys:suspend/1` promises the
/// caller that the process is frozen, and anything the loop does in the
/// meantime breaks that promise.
///
/// ## Examples
///
/// ```gleam
/// case sys.is_suspended(plane) {
///   True -> serve_only_system_messages()
///   False -> serve_everything()
/// }
/// ```
pub fn is_suspended(plane: Plane) -> Bool {
  case plane.mode {
    Suspended -> True
    Running -> False
  }
}

/// Add the system-message arm to a selector.
///
/// The arm matches the `{system, From, Request}` record rather than a
/// subject, so it composes with whatever else the loop selects. Merge it
/// last: `process.merge_selector` lets a later arm replace an earlier one
/// for the same key, and a loop whose user-supplied selector could shadow
/// this arm would silently become invisible to `sys` — the failure mode
/// this module exists to prevent.
///
/// ## Examples
///
/// ```gleam
/// let selector =
///   process.new_selector()
///   |> process.merge_selector(user_selector)
///   |> sys.selecting(System)
/// ```
pub fn selecting(
  selector: Selector(payload),
  mapping wrap: fn(Incoming) -> payload,
) -> Selector(payload) {
  use message <- process.select_record(selector, atom.create("system"), 2)
  wrap(convert_system_message(message))
}

/// Answer a system request against `state`, returning the plane the loop
/// should carry on with.
///
/// Each request is a handshake with a blocked caller, and the reply is sent
/// from inside this function. That ordering is the point: the acknowledgement
/// goes out before the loop touches another message, so by the time
/// `sys:suspend/1` returns to its caller the mode is already `Suspended` and
/// no further mailbox message can be handled. Replying afterwards, from the
/// loop, would leave a window in which the tool believes the process is
/// frozen and the process is still working.
///
/// `get_state` and `get_status` hand the caller an erased view of `state`.
/// This is the only place weft's state leaves the loop, and it is read-only:
/// `replace_state` is reported as `Unimplemented` rather than answered.
///
/// ## Examples
///
/// ```gleam
/// let plane = sys.handle(plane, request, holding: self.state)
/// // -> the caller has its reply; the mode may now be Suspended
/// ```
pub fn handle(
  plane: Plane,
  message: SystemMessage,
  holding state: state,
) -> Plane {
  case message {
    GetState(reply) -> {
      reply(erase(state))
      plane
    }

    GetStatus(reply) -> {
      reply(status_info(plane, state))
      plane
    }

    // The reply is what releases `sys:suspend/1`. The mode change is
    // recorded in the value returned to the loop, so the loop cannot
    // observe a message between the two.
    Suspend(reply) -> {
      reply()
      Plane(..plane, mode: Suspended)
    }

    Resume(reply) -> {
      reply()
      Plane(..plane, mode: Running)
    }
  }
}

/// The observer-shaped status report for this plane.
///
/// The record is `gleam/otp/system`'s so that weft's loops and gleam_otp's
/// actor report identically; the Erlang side turns it into the five-element
/// data list `sys:get_status/1` specifies.
fn status_info(plane: Plane, state: state) -> system.StatusInfo {
  StatusInfo(
    module: plane.module,
    parent: plane.parent,
    mode: plane.mode,
    debug_state: plane.debug_state,
    state: erase(state),
  )
}

/// Shed the process stack and heap, resuming into `continuation` when the
/// next message arrives.
///
/// This never returns. `erlang:hibernate/3` discards the current call stack,
/// so the continuation runs on a fresh one and anything the caller intended
/// to do after this call will not happen — a loop hibernating must therefore
/// pass its whole remaining behaviour as the continuation, and must already
/// have arranged its exit by signal rather than by returning a value up a
/// stack that no longer exists.
///
/// A hibernating process wakes on the arrival of any message, and returns
/// immediately if its mailbox is not empty, so this is safe to call from a
/// loop that has just failed to receive.
///
/// ## Examples
///
/// ```gleam
/// case process.selector_receive(selector, quiet_ms) {
///   Ok(event) -> handle(event)
///   Error(Nil) -> sys.hibernate(fn() { loop(self) })
/// }
/// // -> the loop resumes, on a fresh stack, at the next message
/// ```
@external(erlang, "weft_sys_ffi", "hibernate")
pub fn hibernate(continuation: fn() -> a) -> a

/// Report something the loop could not handle to the OTP logger.
///
/// Weft's loops discard messages they were not built for rather than
/// crashing on them, which is only defensible if the discard is visible.
/// This is the single place the library logs, kept here so that `@external`
/// stays inside the internal modules.
///
/// ## Examples
///
/// ```gleam
/// sys.warn("weft/actor discarding unexpected message: " <> string.inspect(m))
/// // -> logged at warning level
/// ```
pub fn warn(message: String) -> Nil {
  log_warning(charlist.from_string("~s"), [charlist.from_string(message)])
}

/// Normalise a raw `system` message into `Incoming`.
///
/// Total: the Erlang clause head accepts any term, so a malformed or
/// unknown request is reported rather than raised. See `weft_sys_ffi.erl`,
/// which also owns the reply tagging `sys` expects.
@external(erlang, "weft_sys_ffi", "convert_system_message")
fn convert_system_message(message: Dynamic) -> Incoming

/// Erase a value's type for the `sys` reply channel, which is untyped by
/// construction: the debug tool asking for the state has no way to know
/// what type it is.
@external(erlang, "weft_sys_ffi", "identity")
fn erase(value: anything) -> Dynamic

/// The OTP logger. There are no Gleam bindings for it yet, and inventing a
/// logging abstraction for one call site would be worse than this line.
@external(erlang, "logger", "warning")
fn log_warning(format: Charlist, arguments: List(Charlist)) -> Nil
