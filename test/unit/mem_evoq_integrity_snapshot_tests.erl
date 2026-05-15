%%% @doc Unit tests for snapshot integrity (move 16) and subscription
%%% catch-up verification (move 17) on mem_evoq_store.
%%%
%%% Two record types appear here:
%%%
%%%   * `#snapshot{}'      — storage-internal. Carries anchor_hash + mac.
%%%                          Reached via sys:get_state / sys:replace_state
%%%                          in the tamper helpers.
%%%   * `#evoq_snapshot{}' — what the adapter returns after translation.
%%%                          Carries stream_id / version / data /
%%%                          metadata / timestamp only.
%%% @end
-module(mem_evoq_integrity_snapshot_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").
-include_lib("evoq/include/evoq_types.hrl").

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
        "mem_evoq_integrity_snap_test_" ++
        integer_to_list(erlang:unique_integer([positive]))).

with_integrity_store(F) ->
    {StoreId, Key} = setup_with_integrity(),
    try F(StoreId, Key) after cleanup(StoreId) end.

seed(StoreId, StreamId, N) ->
    Events = [#{event_type => <<"e">>, data => #{n => I}}
              || I <- lists:seq(1, N)],
    {ok, _} = mem_evoq_adapter:append(StoreId, StreamId, ?ANY_VERSION, Events),
    ok.

%% Reach into the store and pull the raw #snapshot{} record at a
%% specific (stream, version). Tests need this to inspect anchor_hash
%% + mac, which the adapter strips.
raw_snapshot(StoreId, StreamId, Version) ->
    {ok, Pid} = mem_evoq_registry:lookup(StoreId),
    State = sys:get_state(Pid),
    Snapshots = element(4, State),
    PerStream = maps:get(StreamId, Snapshots),
    maps:get(Version, PerStream).

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

tamper_snapshot(StoreId, StreamId, Version, Fun) ->
    {ok, Pid} = mem_evoq_registry:lookup(StoreId),
    _ = sys:replace_state(Pid, fun(State) ->
        Snapshots = element(4, State),
        PerStream = maps:get(StreamId, Snapshots),
        Snap = maps:get(Version, PerStream),
        NewPerStream = maps:put(Version, Fun(Snap), PerStream),
        setelement(4, State, maps:put(StreamId, NewPerStream, Snapshots))
    end),
    ok.

drain_events(Timeout) ->
    receive
        {events, _} = M -> [M | drain_events(Timeout)];
        {subscription_error, _} = E -> [E | drain_events(Timeout)]
    after Timeout -> []
    end.

%%====================================================================
%% Move 16 — snapshot save populates anchor + mac (storage-side)
%%====================================================================

save_populates_anchor_and_mac_in_storage_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        ok = mem_evoq_adapter:save(
            StoreId, <<"s$1">>, 2, #{state => x}, #{}),
        %% Inspect storage-side record directly — anchor + mac don't
        %% cross the adapter boundary.
        Raw = raw_snapshot(StoreId, <<"s$1">>, 2),
        ?assert(is_binary(Raw#snapshot.anchor_hash)),
        ?assertEqual(32, byte_size(Raw#snapshot.anchor_hash)),
        ?assertMatch({1, _}, Raw#snapshot.mac)
    end).

save_refuses_when_event_absent_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        Result = mem_evoq_adapter:save(
            StoreId, <<"s$1">>, 99, #{}, #{}),
        ?assertMatch({error, {snapshot_anchor_unavailable, _}}, Result)
    end).

%%====================================================================
%% Move 16 — load verifies anchor + mac
%%====================================================================

intact_snapshot_loads_clean_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 5),
        ok = mem_evoq_adapter:save(
            StoreId, <<"s$1">>, 4, #{state => x}, #{}),
        {ok, Loaded} = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertEqual(4, Loaded#evoq_snapshot.version)
    end).

tampered_snapshot_data_caught_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 5),
        ok = mem_evoq_adapter:save(
            StoreId, <<"s$1">>, 4, #{state => x}, #{}),
        ok = tamper_snapshot(StoreId, <<"s$1">>, 4,
            fun(S) -> S#snapshot{data = #{forged => true}} end),
        Result = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertMatch({error, {integrity_violation,
                              #{kind := snapshot_mac_mismatch}}}, Result)
    end).

tampered_snapshot_anchor_caught_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 5),
        ok = mem_evoq_adapter:save(
            StoreId, <<"s$1">>, 4, #{state => x}, #{}),
        ok = tamper_snapshot(StoreId, <<"s$1">>, 4,
            fun(S) -> S#snapshot{anchor_hash = <<99:256>>} end),
        Result = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertMatch({error, {integrity_violation,
                              #{kind := snapshot_anchor_mismatch}}}, Result)
    end).

tampered_underlying_stream_breaks_snapshot_anchor_test() ->
    %% The headline property of snapshot integrity: tampering an
    %% event AFTER a snapshot was taken (even one re-signed with
    %% the legitimate key) breaks the anchor.
    with_integrity_store(fun(StoreId, Key) ->
        seed(StoreId, <<"s$1">>, 5),
        ok = mem_evoq_adapter:save(
            StoreId, <<"s$1">>, 4, #{state => intact}, #{}),
        %% Re-sign event 4 with new data under the legitimate key —
        %% individual MAC will pass but the recomputed chain_hash
        %% no longer matches the snapshot's anchor.
        ok = tamper_event(StoreId, <<"s$1">>, 4,
            fun(E) ->
                E2 = E#event{data = #{tampered => true}, mac = undefined,
                             signature = undefined},
                Mac = reckon_gater_integrity:compute_event_mac(E2, Key),
                E2#event{mac = Mac}
            end),
        Result = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertMatch({error, {integrity_violation,
                              #{kind := snapshot_anchor_mismatch}}}, Result)
    end).

%%====================================================================
%% Move 17 — subscription catch-up verification
%%====================================================================

intact_catchup_delivers_all_events_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, self(), #{from => 0}),
        Msgs = drain_events(200),
        Events = lists:append([Es || {events, Es} <- Msgs]),
        ?assertEqual(3, length(Events)),
        Errors = [E || {subscription_error, _} = E <- Msgs],
        ?assertEqual([], Errors)
    end).

tampered_event_during_catchup_sends_subscription_error_test() ->
    with_integrity_store(fun(StoreId, _Key) ->
        seed(StoreId, <<"s$1">>, 3),
        ok = tamper_event(StoreId, <<"s$1">>, 1,
            fun(E) -> E#event{data = #{forged => true}} end),
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, self(), #{from => 0}),
        Msgs = drain_events(200),
        Errors = [E || {subscription_error, _} = E <- Msgs],
        ?assertMatch([{subscription_error,
                       {integrity_violation, #{kind := mac_mismatch}}}],
                     Errors)
    end).
