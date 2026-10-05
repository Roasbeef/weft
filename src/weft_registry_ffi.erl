%% Only operations absent from the Gleam bindings live here. Registration
%% policy, monitor ownership and cleanup run in the Gleam registry actor.
-module(weft_registry_ffi).
-export([new_table/0, read/2, put/3, delete/2, lookup/2,
         local_owner/1]).

%% The registry actor creates and owns this protected table. Callers may read
%% it, while ETS enforces that only its owner can mutate the rows. Without
%% named_table, the module atom is a label, not a globally registered table.
new_table() -> ets:new(?MODULE, [set, protected, {read_concurrency, true}]).

%% Owner-side reads need no table-deletion recovery: the actor is executing
%% inside the table owner, and no other process can destroy its table.
read(Table, Key) ->
    case ets:lookup(Table, Key) of
        [{Key, Binding}] -> {ok, Binding};
        [] -> {error, nil}
    end.

put(Table, Key, Binding) -> ets:insert(Table, {Key, Binding}), nil.
delete(Table, Key) -> ets:delete(Table, Key), nil.

%% The opaque typed address is the proof that this erased subject has the
%% caller's message type. Table deletion races only this read-only path.
lookup(Table, Key) ->
    try ets:lookup(Table, Key) of
        [{Key, {binding, Pid, Subject, _Monitor}}] ->
            case is_process_alive(Pid) of
                true -> {ok, Subject};
                false -> {error, nil}
            end;
        [] -> {error, nil}
    catch
        error:badarg -> {error, nil}
    end.

%% Only local unnamed subjects carry a stable pid we can monitor here.
%% Named subjects require a separate resolution policy, so reject that shape.
local_owner({subject, Pid, _Tag}) when node(Pid) =:= node() -> {ok, Pid};
local_owner(_Subject) ->
    {error, <<"recipient must be a local unnamed subject">>}.
