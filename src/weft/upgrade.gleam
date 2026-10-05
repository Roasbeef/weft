//// Typed requests for transactional, in-place callback upgrades.
////
//// The trusted loader loads the new BEAM modules before suspending a loop.
//// Migration sees current state, including work completed since an earlier
//// upgrade. It returns a complete callback set; loading a module alone does
//// not replace captured closures. Message and state types remain stable.
////
//// Migration callbacks must be pure. Gleam has no effect system, so this is
//// a trust boundary rather than a sandbox: callbacks can call arbitrary code.
//// Weft isolates computation and bounds its lifetime, but cannot undo effects
//// a dishonest callback performs. Never expose migration to model-authored
//// code running in the harness VM.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import gleam/string
import weft

/// The standard OTP change-code arguments, without casting dynamic state.
pub type Request {
  Request(
    /// The loaded callback module selected by the trusted loader.
    module: Atom,
    /// The loader's old version, including OTP's downgrade tuple if supplied.
    old_version: Dynamic,
    /// Caller-defined metadata, decoded totally by the migration callback.
    extra: Dynamic,
  )
}

/// Compute a candidate on the existing bounded task engine.
///
/// This is for behaviour implementations, not an application upgrade API.
/// The caller retains its original state until the successful candidate is
/// returned. Deadline, rejection and crash are errors, never partial commits.
/// The deadline bounds computation; scheduler and kill/join latency add to it.
///
/// ## Examples
///
/// ```gleam
/// upgrade.prepare(100, fn() { Ok(candidate) })
/// ```
pub fn prepare(
  within: Int,
  compute: fn() -> Result(candidate, String),
) -> Result(candidate, String) {
  case within > 0 {
    False -> Error("migration deadline must be positive")
    True -> {
      case weft.new([compute]) |> weft.deadline(within) |> weft.start {
        [weft.Completed(_, candidate)] -> Ok(candidate)
        [weft.Failed(_, reason)] -> Error(reason)
        outcomes ->
          Error("migration did not complete: " <> string.inspect(outcomes))
      }
    }
  }
}
