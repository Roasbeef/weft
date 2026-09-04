//// Reclaimable addresses for actors that restart inside a long-lived scope.
////
//// An address holds a reference, not an atom or a cached recipient. Binding
//// publishes one live subject; lookup resolves its current incarnation.
//// The registry monitors each recipient and removes its binding on death.
//// A replacement may bind before the old DOWN arrives, so cleanup matches
//// the old monitor rather than deleting whichever binding is current.
////
//// Registries are linked OTP processes with unnamed ETS tables. They own
//// address resolution, not the lifetimes or effects of their recipients.
//// Stop recipients through their supervisor or drain owner before stopping
//// the registry. Its death invalidates every address in that scope.
//// Reads go directly through ETS; ordinary traffic never enters the
//// registry's mailbox. A resolved subject is only a momentary observation:
//// callers must resolve again after a restart and must not cache it.
////
//// ```gleam
//// let assert Ok(names) = registry.start()
//// let address = registry.new_address(names)
//// let assert Ok(started) = actor.new(0)
////   |> actor.addressed(address)
////   |> actor.start
//// assert registry.lookup(address) == Ok(started.data)
//// ```

import gleam/erlang/process.{type Pid, type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/result
import weft/internal/registry as ffi

/// One linked owner of a reclaimable address namespace.
pub type Registry =
  ffi.Registry

/// A typed logical address, valid only while its registry remains alive.
///
/// The message parameter prevents registering a subject with another message
/// type. Each address has a unique reference even before its first binding.
pub opaque type Address(message) {
  Address(registry: Registry, key: Reference)
}

/// Starts a linked registry with an empty, unnamed table.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(names) = registry.start()
/// let address = registry.new_address(names)
/// ```
pub fn start() -> Result(Registry, String) {
  ffi.start()
}

/// Allocates a logical address without adding a table row or an atom.
///
/// ## Examples
///
/// ```gleam
/// let address = registry.new_address(names)
/// assert registry.lookup(address) == Error(Nil)
/// ```
pub fn new_address(registry: Registry) -> Address(message) {
  Address(registry:, key: reference.new())
}

/// Binds an address to one live, local, unnamed subject.
///
/// Registering the same subject twice is idempotent. Another live recipient
/// is a conflict, even if both subjects have the same owner. A dead binding
/// can be replaced immediately; callers need not wait for monitor delivery.
/// The binding cannot prove that the recipient's external effects drained.
/// A timeout reports an unavailable registry, not a revoked request: a
/// queued registration may still bind if its recipient remains alive.
///
/// ## Examples
///
/// ```gleam
/// let inbox = process.new_subject()
/// assert registry.register(address, inbox) == Ok(Nil)
/// assert registry.lookup(address) == Ok(inbox)
/// ```
pub fn register(
  address: Address(message),
  subject: Subject(message),
) -> Result(Nil, String) {
  ffi.register(address.registry, address.key, subject)
}

/// Creates and binds an inbox in the calling process before initialisation.
///
/// A failed initialiser must end that process so its monitor reclaims the
/// binding. Actor builders use this path before acknowledging startup.
///
/// ## Examples
///
/// ```gleam
/// use inbox <- result.try(registry.register_self(address))
/// initialise(inbox)
/// ```
pub fn register_self(
  address: Address(message),
) -> Result(Subject(message), String) {
  let subject = process.new_subject()
  use Nil <- result.try(register(address, subject))
  Ok(subject)
}

/// Resolves the current live recipient without sending a registry request.
///
/// An unbound address and an address whose registry has stopped both return
/// `Error(Nil)`. The recipient can die after lookup, as with any process
/// handle; a successful lookup does not acknowledge subsequent execution.
///
/// ## Examples
///
/// ```gleam
/// use inbox <- result.try(registry.lookup(address))
/// process.send(inbox, message)
/// Ok(Nil)
/// ```
pub fn lookup(address: Address(message)) -> Result(Subject(message), Nil) {
  ffi.lookup(address.registry, address.key)
}

/// Sends to the current recipient, or reports that none is available.
///
/// ## Examples
///
/// ```gleam
/// let sent = registry.send(address, notification)
/// ```
pub fn send(address: Address(message), message: message) -> Result(Nil, Nil) {
  use subject <- result.try(lookup(address))
  process.send(subject, message)
  Ok(Nil)
}

/// Returns the registry process for supervision and lifetime monitoring.
///
/// ## Examples
///
/// ```gleam
/// let monitor = process.monitor(registry.owner(names))
/// ```
pub fn owner(registry: Registry) -> Pid {
  ffi.owner(registry)
}

/// Stops the registry and invalidates all its addresses without stopping
/// their recipients. Callers retain responsibility for recipient shutdown.
///
/// ## Examples
///
/// ```gleam
/// assert registry.stop(names) == Ok(Nil)
/// assert registry.lookup(address) == Error(Nil)
/// ```
pub fn stop(registry: Registry) -> Result(Nil, String) {
  ffi.stop(registry)
}
