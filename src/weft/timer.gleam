//// Where a weft loop's timeouts come from.
////
//// Every weft loop that answers a timeout — the actor's loop timeout and
//// heartbeat, the state machine's four timeout kinds — arms it through the
//// timer book, and the book has always armed with `erlang:send_after` on
//// the wall clock. For a loop waiting on a real resource that is the only
//// honest source: a socket does not go quiet on logical time.
////
//// Some loops are not waiting on a resource. A session whose whole liveness
//// rides an injected time capability runs its waits on a clock its own
//// simulation steps, and its tests drive that clock by hand — pop the
//// earliest arming, run it, assert what moved. Such a consumer cannot use a
//// named timeout at all if the book reaches past it to the wall clock: the
//// deterministic run becomes non-deterministic, and its tests either sleep
//// or fail. What it does instead is hand-roll the book — a tick message
//// carrying a generation, a re-arm, a stale check — which is the same race
//// solved a second time, worse.
////
//// `Source` is that decision made a value. `WallClock` is what every loop
//// uses unless told otherwise; `Injected` hands the arming to the caller's
//// own `after`. The injected shape is deliberately the shape a host
//// system's time capability already has — `fn(delay_ms, wake) -> Nil`, one
//// call, no handle, no result — so a consumer passes that capability
//// straight in rather than writing an adapter around it.
////
//// ## What an injected source is held to, and what it is not
////
//// The contract is as weak as it can be, because a weak contract is what
//// makes a fake timer wheel or a logical clock a legal source. `after`
//// must arrange for `wake` to be called no earlier than `delay_ms` on its
//// own time base, and must return promptly rather than blocking the loop
//// that armed it. `wake` may be called from any process. Beyond that:
////
//// - **Late is fine.** The book stamps armings; it does not measure them.
//// - **Twice is fine.** A generation is delivered at most once, because the
////   book deletes the entry as it hands the payload on. The second wake for
////   the same arming is stale.
//// - **Never is a liveness cost bounded to that arming.** A wake that is
////   dropped is one timeout that does not fire; nothing else in the loop is
////   affected, and arming the key again restores the series.
////
//// What an injected source gives up is cancellation. `erlang:send_after`
//// yields a handle the book keeps and uses, so a superseded arming is
//// usually stopped before it ever fires. An `after` call yields nothing, so
//// under `Injected` **the book cannot stop an arming it has cancelled or
//// replaced**: every such wake still arrives, and is dropped by the
//// generation check when the loop accepts it. That check was written for
//// the wall clock's own unclosable race — a timer that fires in the window
//// before its cancel runs — and it is exactly what makes an uncancellable
//// source safe rather than merely tolerable.
////
//// ## Example
////
//// ```gleam
//// import weft/state_machine as sm
//// import weft/timer
////
//// // A machine whose timeouts are armed on the session's own time base, so
//// // that a simulated run, and the tests that drive it, decide when they
//// // fire.
//// sm.new(Idle, session)
//// |> sm.with_timer_source(timer.Injected(after: session_after))
//// |> sm.on_event(handle)
//// |> sm.start
//// ```

/// How a timer book arms its timers.
///
/// The two shapes differ in one observable way beyond whose clock they
/// measure: `WallClock` keeps a cancel handle and `Injected` has none. This
/// module's header has what that costs and why it is safe.
///
/// ## Examples
///
/// ```gleam
/// // A fake wheel a test steps by hand: each arming is recorded, and
/// // nothing fires until the test runs one of the recorded wakes.
/// timer.Injected(after: fn(delay_ms, wake) {
///   process.send(armings, Arming(delay_ms:, wake:))
/// })
/// ```
pub type Source {
  /// `erlang:send_after` on the wall clock, via `process.send_after`.
  ///
  /// The default for every loop, and the only source that can stop an
  /// arming it has superseded: the handle comes back from the arming call
  /// and the book keeps it. No process is spawned per timer — the BEAM's
  /// own timer wheel does the work.
  WallClock

  /// A caller-supplied arming function.
  ///
  /// One call per arming, and the book keeps nothing back from it. A
  /// cancelled or replaced arming's `wake` still arrives and dies in the
  /// book's generation check, so `accept` is the whole of the defence
  /// under this source.
  Injected(
    /// Arrange for `wake` to be called once, no earlier than `delay_ms`
    /// milliseconds on this source's own time base, from any process.
    /// Must return promptly: the loop that armed the timer is blocked
    /// until it does. Calling `wake` late, or more than once, is harmless;
    /// never calling it costs liveness for that one arming.
    after: fn(Int, fn() -> Nil) -> Nil,
  )
}
