%%% @doc Unit tests for read_all_global/3 — the cross-stream
%%% reader sorted by epoch_us. Used by catch-up subscriptions.
%%% @end
-module(mem_evoq_read_all_global_tests).

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
        "mem_evoq_read_all_global_test_" ++
        integer_to_list(erlang:unique_integer([positive]))).

with_store(F) ->
    StoreId = setup(),
    try F(StoreId) after cleanup(StoreId) end.

seed(StoreId, StreamId, Tag, N) ->
    Events = [#{event_type => Tag, data => #{stream => StreamId, n => I}}
              || I <- lists:seq(1, N)],
    {ok, _} = mem_evoq_adapter:append(StoreId, StreamId, ?NO_STREAM, Events),
    ok.

%%====================================================================
%% Empty store
%%====================================================================

read_all_global_on_empty_store_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual({ok, []},
                     mem_evoq_adapter:read_all_global(StoreId, 0, 100))
    end).

%%====================================================================
%% Sort order is epoch_us ascending
%%====================================================================

read_all_global_returns_events_in_epoch_order_test() ->
    with_store(fun(StoreId) ->
        %% Three streams seeded in sequence. Because epoch_us is set
        %% per-call to system_time(microsecond), events from stream A
        %% (written first) will have lower epoch_us than B than C.
        seed(StoreId, <<"a$1">>, <<"e">>, 3),
        seed(StoreId, <<"b$1">>, <<"e">>, 3),
        seed(StoreId, <<"c$1">>, <<"e">>, 3),
        {ok, All} = mem_evoq_adapter:read_all_global(StoreId, 0, 100),
        ?assertEqual(9, length(All)),
        %% Each stream's events are batched together (all 3 b's get
        %% the same epoch_us because they're written in a single
        %% append/4 call). We assert the cross-stream ordering: the
        %% first 3 are stream a, the next 3 are b, the last 3 are c.
        StreamIds = [E#evoq_event.stream_id || E <- All],
        FirstThree = lists:sublist(StreamIds, 1, 3),
        MiddleThree = lists:sublist(StreamIds, 4, 3),
        LastThree = lists:sublist(StreamIds, 7, 3),
        ?assertEqual([<<"a$1">>, <<"a$1">>, <<"a$1">>], FirstThree),
        ?assertEqual([<<"b$1">>, <<"b$1">>, <<"b$1">>], MiddleThree),
        ?assertEqual([<<"c$1">>, <<"c$1">>, <<"c$1">>], LastThree)
    end).

%%====================================================================
%% Offset
%%====================================================================

read_all_global_skips_offset_events_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"a$1">>, <<"e">>, 3),
        seed(StoreId, <<"b$1">>, <<"e">>, 3),
        seed(StoreId, <<"c$1">>, <<"e">>, 3),
        {ok, Skipped} = mem_evoq_adapter:read_all_global(StoreId, 5, 100),
        %% 9 total - 5 offset = 4 remaining
        ?assertEqual(4, length(Skipped))
    end).

read_all_global_offset_past_end_returns_empty_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"a$1">>, <<"e">>, 3),
        {ok, []} = mem_evoq_adapter:read_all_global(StoreId, 100, 10)
    end).

%%====================================================================
%% Batch size
%%====================================================================

read_all_global_respects_batch_size_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"a$1">>, <<"e">>, 5),
        seed(StoreId, <<"b$1">>, <<"e">>, 5),
        {ok, Batch} = mem_evoq_adapter:read_all_global(StoreId, 0, 3),
        ?assertEqual(3, length(Batch))
    end).

read_all_global_offset_plus_batch_yields_remaining_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"a$1">>, <<"e">>, 5),
        seed(StoreId, <<"b$1">>, <<"e">>, 5),
        %% Skip 3, take 3 → events at positions 4, 5, 6 (1-indexed).
        {ok, Batch} = mem_evoq_adapter:read_all_global(StoreId, 3, 3),
        ?assertEqual(3, length(Batch))
    end).

%%====================================================================
%% Single-stream store
%%====================================================================

read_all_global_with_single_stream_returns_stream_order_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"only$1">>, <<"e">>, 5),
        {ok, All} = mem_evoq_adapter:read_all_global(StoreId, 0, 100),
        ?assertEqual(5, length(All)),
        Versions = [E#evoq_event.version || E <- All],
        ?assertEqual([0,1,2,3,4], Versions)
    end).
