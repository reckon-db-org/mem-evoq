%%% @doc Unit tests for snapshot operations on mem_evoq_store.
%%%
%%% Covers the full snapshot lifecycle: save (record + 5-arg builder),
%%% load (latest + at-version), list, delete. Integrity-aware snapshot
%%% behaviour lands in later moves (14-17).
%%% @end
-module(mem_evoq_snapshot_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").

setup() ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    StoreId = unique_store_id(),
    {ok, _} = mem_evoq:start_store(StoreId),
    StoreId.

cleanup(StoreId) ->
    ok = mem_evoq:stop_store(StoreId).

unique_store_id() ->
    list_to_atom(
        "mem_evoq_snapshot_test_" ++
        integer_to_list(erlang:unique_integer([positive]))).

with_store(F) ->
    StoreId = setup(),
    try F(StoreId) after cleanup(StoreId) end.

mk_snap(StreamId, Version, Data) ->
    #snapshot{
        stream_id = StreamId,
        version = Version,
        data = Data,
        metadata = #{},
        timestamp = erlang:system_time(millisecond)
    }.

%%====================================================================
%% Save + load round-trip
%%====================================================================

save_then_load_roundtrip_test() ->
    with_store(fun(StoreId) ->
        Snap = mk_snap(<<"s$1">>, 5, #{state => running, n => 7}),
        ok = mem_evoq_adapter:save_snapshot(StoreId, <<"s$1">>, Snap),
        {ok, Loaded} = mem_evoq_adapter:load_snapshot(StoreId, <<"s$1">>),
        ?assertEqual(<<"s$1">>, Loaded#snapshot.stream_id),
        ?assertEqual(5, Loaded#snapshot.version),
        ?assertEqual(#{state => running, n => 7}, Loaded#snapshot.data)
    end).

save_snapshot_with_5_arg_builder_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, 5, #{state => x}, #{trace => <<"t-1">>}),
        {ok, Loaded} = mem_evoq_adapter:load_snapshot(StoreId, <<"s$1">>),
        ?assertEqual(5, Loaded#snapshot.version),
        ?assertEqual(#{state => x}, Loaded#snapshot.data),
        ?assertEqual(#{trace => <<"t-1">>}, Loaded#snapshot.metadata),
        ?assert(is_integer(Loaded#snapshot.timestamp))
    end).

%%====================================================================
%% Latest version selection
%%====================================================================

load_returns_latest_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 3, #{state => a})),
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 7, #{state => b})),
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 5, #{state => c})),
        {ok, Loaded} = mem_evoq_adapter:load_snapshot(StoreId, <<"s$1">>),
        ?assertEqual(7, Loaded#snapshot.version)
    end).

load_at_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 3, #{state => a})),
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 5, #{state => b})),
        {ok, Loaded} = mem_evoq_adapter:load_snapshot_at(
            StoreId, <<"s$1">>, 3),
        ?assertEqual(#{state => a}, Loaded#snapshot.data)
    end).

load_at_nonexistent_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 3, #{})),
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:load_snapshot_at(StoreId, <<"s$1">>, 99))
    end).

%%====================================================================
%% Not-found cases
%%====================================================================

load_latest_from_empty_store_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:load_snapshot(StoreId, <<"missing$1">>))
    end).

load_latest_from_stream_with_no_snapshots_test() ->
    %% Stream exists in event history but has no snapshot.
    with_store(fun(StoreId) ->
        {ok, _} = mem_evoq_adapter:append(
            StoreId, <<"s$1">>, ?NO_STREAM,
            [#{event_type => <<"e">>, data => #{}}]),
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:load_snapshot(StoreId, <<"s$1">>))
    end).

%%====================================================================
%% Overwrite same-version snapshot
%%====================================================================

save_replaces_snapshot_at_same_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 5, #{v => 1})),
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 5, #{v => 2})),
        {ok, Loaded} = mem_evoq_adapter:load_snapshot(StoreId, <<"s$1">>),
        ?assertEqual(#{v => 2}, Loaded#snapshot.data)
    end).

%%====================================================================
%% List + delete
%%====================================================================

list_snapshots_returns_oldest_first_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 7, #{})),
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 3, #{})),
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 5, #{})),
        {ok, Snaps} = mem_evoq_adapter:list_snapshots(StoreId, <<"s$1">>),
        Versions = [S#snapshot.version || S <- Snaps],
        ?assertEqual([3, 5, 7], Versions)
    end).

list_snapshots_empty_for_unknown_stream_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual({ok, []},
                     mem_evoq_adapter:list_snapshots(StoreId, <<"none$1">>))
    end).

delete_snapshots_for_stream_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 5, #{})),
        ok = mem_evoq_adapter:delete_snapshot(StoreId, <<"s$1">>),
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:load_snapshot(StoreId, <<"s$1">>))
    end).

delete_snapshot_idempotent_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual(ok, mem_evoq_adapter:delete_snapshot(StoreId, <<"none$1">>))
    end).

%%====================================================================
%% Stream delete cascades to snapshots
%%====================================================================

stream_delete_drops_snapshots_too_test() ->
    with_store(fun(StoreId) ->
        {ok, _} = mem_evoq_adapter:append(
            StoreId, <<"s$1">>, ?NO_STREAM,
            [#{event_type => <<"e">>, data => #{}}]),
        ok = mem_evoq_adapter:save_snapshot(
            StoreId, <<"s$1">>, mk_snap(<<"s$1">>, 0, #{state => x})),
        ok = mem_evoq_adapter:delete(StoreId, <<"s$1">>),
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:load_snapshot(StoreId, <<"s$1">>))
    end).
