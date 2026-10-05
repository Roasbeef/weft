%% Fixed test-only node roles. No production callback or RPC surface is added.
-module(weft_remote_consumer_test_ffi).
-export([probe/0, owner/0, consumer/0, lose/2]).

probe() ->
    try
        Root = code:root_dir(),
        Erl = filename:join([Root, "erts-" ++ erlang:system_info(version), "bin", "erl"]),
        Paths = [filename:absname(P) || P <- code:get_path()],
        Name = "weft_watch_" ++ integer_to_list(erlang:system_time(nanosecond)) ++ "@127.0.0.1",
        Port = open_port({spawn_executable, Erl},
            [binary, exit_status, stderr_to_stdout,
             {args, ["+S", "2", "-pa"] ++ Paths ++
                ["-name", Name, "-setcookie", "weft_remote_watch_fixture",
                 "-noshell", "-eval", "weft_remote_consumer_test_ffi:owner()."]}]),
        try collect(Port, <<>>)
        after
            try port_close(Port) catch error:badarg -> ok end
        end
    catch _:_ -> {error, nil} end.

collect(Port, Acc) ->
    receive
        {Port, {data, Bytes}} when byte_size(Acc) + byte_size(Bytes) =< 65536 ->
            collect(Port, <<Acc/binary, Bytes/binary>>);
        {Port, {exit_status, 0}} ->
            case binary:match(Acc, <<"WEFT_REMOTE_WATCH_COMPLETE">>) of
                nomatch -> {error, nil};
                _ -> {ok, nil}
            end;
        {Port, _} -> {error, nil}
    after 15000 -> {error, nil}
    end.

owner() ->
    %% This isolated fixture VM cannot outlive its finite OS-runner budget.
    {ok, _} = timer:apply_after(12000, erlang, halt, [2]),
    try
        ok = application:set_env(kernel, dist_auto_connect, never),
        Paths = [filename:absname(P) || P <- code:get_path()],
        Name = "weft_consumer_" ++ integer_to_list(erlang:unique_integer([positive])),
        {ok, Control, Node} = peer:start_link(#{name => Name,
            host => "127.0.0.1", longnames => true, connection => standard_io,
            args => ["+S", "2", "-setcookie", "weft_remote_watch_fixture", "-pa"] ++ Paths}),
        try
            true = net_kernel:connect_node(Node),
            Pid1 = peer:call(Control, ?MODULE, consumer, [], 5000),
            'weft_remote_consumer_test':exercise(Pid1, 0),
            Pid2 = peer:call(Control, ?MODULE, consumer, [], 5000),
            'weft_remote_consumer_test':exercise(Pid2, 1)
        after peer:stop(Control) end,
        io:format("WEFT_REMOTE_WATCH_COMPLETE~n"),
        halt(0)
    catch Class:Reason:Stack ->
        io:format("~p:~p ~p~n", [Class, Reason, Stack]), halt(1)
    end.

consumer() -> spawn(fun() -> receive stop -> ok end end).

lose(Pid, 0) -> Pid ! stop, nil;
lose(Pid, 1) -> true = erlang:disconnect_node(node(Pid)), nil.
