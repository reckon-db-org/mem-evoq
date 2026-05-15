%%% @doc Unit tests for snapshot operations on mem_evoq_store.
%%%
%%% Exercises the `evoq_snapshot_adapter' behaviour surface:
%%% save / read / read_at_version / delete / delete_at_version /
%%% list_versions. Adapter-returned snapshots are `#evoq_snapshot{}'
%%% records; the storage-side `#snapshot{}' carries anchor_hash +
%%% mac and is reached only via sys:get_state by integrity tests.
%%% @end
-module(mem_evoq_snapshot_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").
-include_lib("evoq/include/evoq_types.hrl").

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

%%====================================================================
%% Save + read round-trip
%%====================================================================

save_then_read_roundtrip_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(
            StoreId, <<"s$1">>, 5, #{state => running, n => 7}, #{}),
        {ok, Loaded} = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertEqual(<<"s$1">>, Loaded#evoq_snapshot.stream_id),
        ?assertEqual(5, Loaded#evoq_snapshot.version),
        ?assertEqual(#{state => running, n => 7}, Loaded#evoq_snapshot.data)
    end).

save_populates_metadata_and_timestamp_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(
            StoreId, <<"s$1">>, 5, #{state => x}, #{trace => <<"t-1">>}),
        {ok, Loaded} = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertEqual(5, Loaded#evoq_snapshot.version),
        ?assertEqual(#{state => x}, Loaded#evoq_snapshot.data),
        ?assertEqual(#{trace => <<"t-1">>}, Loaded#evoq_snapshot.metadata),
        ?assert(is_integer(Loaded#evoq_snapshot.timestamp))
    end).

%%====================================================================
%% Latest version selection
%%====================================================================

read_returns_latest_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 3, #{state => a}, #{}),
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 7, #{state => b}, #{}),
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 5, #{state => c}, #{}),
        {ok, Loaded} = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertEqual(7, Loaded#evoq_snapshot.version)
    end).

read_at_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 3, #{state => a}, #{}),
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 5, #{state => b}, #{}),
        {ok, Loaded} = mem_evoq_adapter:read_at_version(StoreId, <<"s$1">>, 3),
        ?assertEqual(#{state => a}, Loaded#evoq_snapshot.data)
    end).

read_at_nonexistent_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 3, #{}, #{}),
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:read_at_version(StoreId, <<"s$1">>, 99))
    end).

%%====================================================================
%% Not-found cases
%%====================================================================

read_from_empty_store_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:read(StoreId, <<"missing$1">>))
    end).

read_from_stream_with_no_snapshots_test() ->
    %% Stream exists in event history but has no snapshot.
    with_store(fun(StoreId) ->
        {ok, _} = mem_evoq_adapter:append(
            StoreId, <<"s$1">>, ?NO_STREAM,
            [#{event_type => <<"e">>, data => #{}}]),
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:read(StoreId, <<"s$1">>))
    end).

%%====================================================================
%% Overwrite same-version snapshot
%%====================================================================

save_replaces_snapshot_at_same_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 5, #{v => 1}, #{}),
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 5, #{v => 2}, #{}),
        {ok, Loaded} = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertEqual(#{v => 2}, Loaded#evoq_snapshot.data)
    end).

%%====================================================================
%% list_versions + delete
%%====================================================================

list_versions_returns_ascending_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 7, #{}, #{}),
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 3, #{}, #{}),
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 5, #{}, #{}),
        ?assertEqual({ok, [3, 5, 7]},
                     mem_evoq_adapter:list_versions(StoreId, <<"s$1">>))
    end).

list_versions_empty_for_unknown_stream_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual({ok, []},
                     mem_evoq_adapter:list_versions(StoreId, <<"none$1">>))
    end).

delete_all_snapshots_for_stream_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 5, #{}, #{}),
        ok = mem_evoq_adapter:delete(StoreId, <<"s$1">>),
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:read(StoreId, <<"s$1">>))
    end).

delete_at_version_test() ->
    with_store(fun(StoreId) ->
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 3, #{v => a}, #{}),
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 5, #{v => b}, #{}),
        ok = mem_evoq_adapter:delete_at_version(StoreId, <<"s$1">>, 5),
        %% 5 is gone, 3 remains.
        ?assertEqual({ok, [3]},
                     mem_evoq_adapter:list_versions(StoreId, <<"s$1">>)),
        {ok, Loaded} = mem_evoq_adapter:read(StoreId, <<"s$1">>),
        ?assertEqual(3, Loaded#evoq_snapshot.version)
    end).

delete_at_version_idempotent_for_missing_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual(ok, mem_evoq_adapter:delete_at_version(
            StoreId, <<"none$1">>, 99))
    end).

delete_all_idempotent_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual(ok, mem_evoq_adapter:delete(StoreId, <<"none$1">>))
    end).

%%====================================================================
%% Stream delete cascades to snapshots
%%====================================================================

stream_delete_drops_snapshots_too_test() ->
    with_store(fun(StoreId) ->
        {ok, _} = mem_evoq_adapter:append(
            StoreId, <<"s$1">>, ?NO_STREAM,
            [#{event_type => <<"e">>, data => #{}}]),
        ok = mem_evoq_adapter:save(StoreId, <<"s$1">>, 0, #{state => x}, #{}),
        ok = mem_evoq_adapter:delete_stream(StoreId, <<"s$1">>),
        ?assertEqual({error, not_found},
                     mem_evoq_adapter:read(StoreId, <<"s$1">>))
    end).
