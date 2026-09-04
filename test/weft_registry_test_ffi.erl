%% Test-only observations of the actual ETS and monitor ledger. Replaying
%% a captured DOWN exercises stale cleanup without a scheduler assumption.
-module(weft_registry_test_ffi).
-export([stats/1, first_down/1, replay_down/2, atom_count/0,
         queue_registration/2, finish_registration/2,
         suspend/1, resume/1, caller_monitor_count/0]).

suspend({registry, _Inbox, _Table, Owner}) -> sys:suspend(Owner), nil.
resume({registry, _Inbox, _Table, Owner}) -> sys:resume(Owner), nil.
caller_monitor_count() ->
    {monitors, Monitors} = process_info(self(), monitors),
    length(Monitors).

stats({registry, _Inbox, Table, Pid}) ->
    {state, Table, Monitors} = sys:get_state(Pid),
    {ets:info(Table, size), maps:size(Monitors)}.

first_down({registry, _Inbox, Table, Owner}) ->
    {state, Table, Monitors} = sys:get_state(Owner),
    [{Monitor, Key}] = maps:to_list(Monitors),
    [{Key, {binding, Pid, _Subject, Monitor}}] = ets:lookup(Table, Key),
    {'DOWN', Monitor, process, Pid, killed}.

replay_down({registry, _Inbox, _Table, Owner}, Down) ->
    Owner ! Down,
    nil.

atom_count() -> erlang:system_info(atom_count).

%% Hold callback dispatch, then queue the replacement before the recipient
%% is killed. The system reply is a same-sender delivery barrier: the call
%% is already in the mailbox when the test causes the old DOWN.
queue_registration({address, {registry, Inbox, _Table, Owner}, Key},
                   {subject, Pid, _Tag} = Subject) ->
    ok = sys:suspend(Owner),
    Reply = 'gleam@erlang@process':new_subject(),
    'gleam@erlang@process':send(Inbox, {bind, Key, Pid, Subject, Reply}),
    _Status = sys:get_status(Owner),
    Reply.

finish_registration({registry, _Inbox, _Table, Owner}, ReplySubject) ->
    ok = sys:resume(Owner),
    {ok, Reply} = 'gleam@erlang@process':'receive'(ReplySubject, 1000),
    Reply.
