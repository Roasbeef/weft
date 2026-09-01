%% weft_sys_ffi — the Erlang side of weft/internal/sys.
%%
%% Three things live here, all of them shapes Gleam cannot express: the
%% normalisation of an OTP `system` message into a Gleam variant, the reply
%% protocol `sys` expects (including the alias tagging OTP 24+ uses), and
%% `erlang:hibernate/3`, which never returns and so has no Gleam signature
%% that could be honest about it.
%%
%% Everything here is total by construction. `convert_system_message/1`
%% answers for any term at all rather than raising a function clause, which
%% is what lets weft/internal/sys promise that a malformed `system` message
%% is reported to the loop instead of killing it.
-module(weft_sys_ffi).

-export([convert_system_message/1, identity/1, hibernate/1]).

identity(X) -> X.

%% Normalise a `{system, From, Request}` message into weft/internal/sys's
%% `Incoming` type: `{request, SystemMessage}` for a request this library
%% implements, `{unimplemented, Term}` for everything else.
%%
%% The callback closed over in each request is the reply. `sys:suspend/1`
%% and friends block their caller until it runs, so the loop calling the
%% callback is what releases the debug tool -- see the ordering note in
%% weft/internal/sys's `handle`.
%%
%% The system messages OTP defines that weft does not implement yet, and so
%% reports as `unimplemented`:
%%   {replace_state, StateFn}
%%   {change_code, Mod, Vsn, Extra}
%%   {terminate, Reason}
%%   {debug, _}
convert_system_message({system, {From, Ref}, Request}) when is_pid(From) ->
    Reply = fun(Msg) ->
        case Ref of
            [alias | Alias] = Tag when is_reference(Alias) ->
                erlang:send(Alias, {Tag, Msg});
            [[alias | Alias] | _] = Tag when is_reference(Alias) ->
                erlang:send(Alias, {Tag, Msg});
            _ ->
                erlang:send(From, {Ref, Msg})
        end,
        nil
    end,
    case Request of
        get_status ->
            {request, {get_status, fun(Status) -> Reply(process_status(Status)) end}};
        get_state ->
            {request, {get_state, fun(State) -> Reply({ok, State}) end}};
        suspend ->
            {request, {suspend, fun() -> Reply(ok) end}};
        resume ->
            {request, {resume, fun() -> Reply(ok) end}};
        Other ->
            {unimplemented, Other}
    end;
convert_system_message(Other) ->
    {unimplemented, Other}.

%% Render a Gleam `system.StatusInfo` record into the five-element data list
%% `sys:get_status/1` and the observer expect. The header and the two data
%% groups are what the observer's process view renders, so a behaviour that
%% skips them shows up as an unlabelled process.
process_status({status_info, Module, Parent, Mode, DebugState, State}) ->
    Data = [
        get(),
        Mode,
        Parent,
        DebugState,
        [
            {header, "Status for Gleam process " ++ pid_to_list(self())},
            {data, [
                {"Gleam behaviour", Module},
                {"Status", Mode},
                {"Parent", Parent}
            ]},
            {data, [{"State", State}]}
        ]
    ],
    {status, self(), {module, Module}, Data}.

%% Shed the process stack and heap, resuming into Continue when the next
%% message arrives. This never returns: the current call stack is discarded,
%% so anything the caller meant to do after it will not happen.
hibernate(Continue) ->
    erlang:hibernate(erlang, apply, [Continue, []]).
