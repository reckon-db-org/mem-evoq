%%% @doc Unit tests for the integrity-enabled write + read path.
%%%
%%% Covers moves 14 + 15: events written under an integrity-enabled
%%% store carry prev_event_hash + mac; reads verify both; tampered
%%% events surface {error, {integrity_violation, _}} same shape as
%%% reckon-db.
%%% @end
-module(mem_evoq_integrity_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").

%%====================================================================
%% Fixture
%%====================================================================

setup_with_integrity() ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    StoreId = unique_store_id(),
    Key = crypto:strong_rand_bytes(32),
    {ok, _} = mem_evoq:start_store(StoreId, #{
        integrity => #{enabled => true, key => Key}
    }),
    {StoreId, Key}.

cleanup(StoreId) ->
    ok = mem_evoq:stop_store(StoreId).

unique_store_id() ->
    list_to_atom(
        "mem_evoq_integrity_test_" ++
        integer_to_list(erlang:unique_integer([positive]))).

with_integrity_store(F) ->
    {StoreId, Key} = setup_with_integrity(),
    try F(StoreId, Key) after cleanup(StoreId) end.

seed(StoreId, StreamId, N) ->
    Events = [#{event_type => <<"e">>, data => #{n => I}}
              || I <- lists:seq(1, N)],
    {ok, _} = mem_evoq_adapter:append(StoreId, StreamId, ?ANY_VERSION, Events),
    ok.

%% Reach into the store's state to tamper an event directly — what an
%% on-disk attacker would do, bypassing all the API guards.
%% sys:replace_state returns the new state; we discard it here and
%% rely on the test's later read to observe the mutation.
tamper_event(StoreId, StreamId, Version, Fun) ->
    {ok, Pid} = mem_evoq_registry:lookup(StoreId),
    _ = sys:replace_state(Pid, fun(State) ->
        Streams = element(3, State),
        Events = maps:get(StreamId, Streams),
        NewEvents = [case E#event.version of
                         Version -> Fun(E);
                         _ -> E
                     end || E <- Events],
        setelement(3, State, maps:put(StreamId, NewEvents, Streams))
    end),
    ok.

%%====================================================================
%% Move 14 — write-path populates integrity fields
%%====================================================================

write_populates_prev_event_hash_and_mac_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward),
        ?assertEqual(3, length(Events)),
        [?assert(is_binary(E#event.prev_event_hash)) || E <- Events],
        [?assertMatch({1, _}, E#event.mac) || E <- Events],
        [?assertEqual(32, byte_size(element(2, E#event.mac))) || E <- Events]
    end).

first_event_prev_hash_is_genesis_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 1),
        {ok, [E]} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 1, forward),
        ?assertEqual(reckon_gater_integrity:genesis_prev_hash(),
                     E#event.prev_event_hash)
    end).

chain_continuity_across_batches_test() ->
    with_integrity_store(fun(StoreId, Key) ->
        seed(StoreId, <<"s$1">>, 2),
        seed(StoreId, <<"s$1">>, 3),  %% second batch
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward),
        ?assertEqual(5, length(Events)),
        %% Walk the chain manually with the key — every link must verify.
        Genesis = reckon_gater_integrity:genesis_prev_hash(),
        walk_and_verify(Events, Genesis, Key)
    end).

walk_and_verify([], _Tip, _Key) -> ok;
walk_and_verify([E | Rest], Tip, Key) ->
    ?assertEqual(ok, reckon_gater_integrity:verify_event(E, Tip, Key)),
    NextTip = reckon_gater_integrity:compute_chain_hash(E, Tip),
    walk_and_verify(Rest, NextTip, Key).

watermark_recorded_on_first_append_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 1),
        {ok, Pid} = mem_evoq_registry:lookup(StoreId),
        State = sys:get_state(Pid),
        %% #state.integrity is field 6
        Integrity = element(6, State),
        ?assertMatch(#{chain_start := #{<<"s$1">> := 0}}, Integrity)
    end).

%%====================================================================
%% Move 14 — disabled store still has no integrity fields
%%====================================================================

disabled_store_writes_no_integrity_fields_test() ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    StoreId = unique_store_id(),
    {ok, _} = mem_evoq:start_store(StoreId),  %% no integrity opts
    try
        seed(StoreId, <<"s$1">>, 2),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward),
        [?assertEqual(undefined, E#event.prev_event_hash) || E <- Events],
        [?assertEqual(undefined, E#event.mac) || E <- Events]
    after
        cleanup(StoreId)
    end.

%%====================================================================
%% Move 15 — read-path verification
%%====================================================================

intact_chain_reads_clean_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward),
        ?assertEqual(5, length(Events))
    end).

tampered_data_caught_on_read_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        ok = tamper_event(StoreId, <<"s$1">>, 1,
            fun(E) -> E#event{data = #{forged => true}} end),
        Result = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward),
        ?assertMatch({error, {integrity_violation,
                              #{kind := mac_mismatch, version := 1}}},
                     Result)
    end).

tampered_prev_event_hash_caught_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        ok = tamper_event(StoreId, <<"s$1">>, 1,
            fun(E) -> E#event{prev_event_hash = <<7:256>>} end),
        Result = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward),
        ?assertMatch({error, {integrity_violation,
                              #{kind := chain_mismatch}}},
                     Result)
    end).

tampered_mac_caught_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        ok = tamper_event(StoreId, <<"s$1">>, 1,
            fun(#event{mac = {KeyId, _}} = E) ->
                E#event{mac = {KeyId, <<0:256>>}}
            end),
        Result = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward),
        ?assertMatch({error, {integrity_violation,
                              #{kind := mac_mismatch}}},
                     Result)
    end).

%%====================================================================
%% Move 15 — backward reads catch the same tampers
%%====================================================================

backward_read_catches_tampering_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        ok = tamper_event(StoreId, <<"s$1">>, 1,
            fun(E) -> E#event{data = #{forged => true}} end),
        ?assertMatch({error, {integrity_violation, _}},
            mem_evoq_adapter:read(StoreId, <<"s$1">>, 2, 3, backward))
    end).

backward_read_intact_returns_descending_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 4, 3, backward),
        Versions = [E#event.version || E <- Events],
        ?assertEqual([4, 3, 2], Versions)
    end).

%%====================================================================
%% Move 15 — skip_all bypasses verification
%%====================================================================

skip_all_returns_tampered_events_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        ok = tamper_event(StoreId, <<"s$1">>, 1,
            fun(E) -> E#event{data = #{forged => true}} end),
        %% Strict-default would error; skip_all bypasses.
        {ok, Pid} = mem_evoq_registry:lookup(StoreId),
        %% Use the 6-arg call via the store gen_server directly since
        %% the 4-arg adapter:read doesn't expose Opts. We test via the
        %% direct call to lock down the verification mode behaviour.
        Result = gen_server:call(Pid,
            {read, <<"s$1">>, 0, 10, forward, #{verify => skip_all}}),
        ?assertMatch({ok, [_,_,_]}, Result)
    end).
