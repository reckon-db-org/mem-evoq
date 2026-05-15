%%% @doc Unit tests for read/5 on mem_evoq_store.
%%%
%%% Mirrors the semantic surface of reckon_db_streams:read/5:
%%% forward + backward, version-bounded, count-bounded, with
%%% stream-not-found surfaced as {error, {stream_not_found, _}}.
%%% @end
-module(mem_evoq_read_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").
-include_lib("evoq/include/evoq_types.hrl").

%%====================================================================
%% Fixture
%%====================================================================

setup() ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    StoreId = unique_store_id(),
    {ok, _} = mem_evoq:start_store(StoreId),
    StoreId.

cleanup(StoreId) ->
    ok = mem_evoq:stop_store(StoreId).

unique_store_id() ->
    list_to_atom(
        "mem_evoq_read_test_" ++
        integer_to_list(erlang:unique_integer([positive]))).

with_store(F) ->
    StoreId = setup(),
    try F(StoreId) after cleanup(StoreId) end.

%% Seed a stream with N events numbered 1..N.
seed(StoreId, StreamId, N) ->
    Events = [#{event_type => <<"e">>, data => #{n => I}}
              || I <- lists:seq(1, N)],
    {ok, _LastVersion} = mem_evoq_adapter:append(
        StoreId, StreamId, ?NO_STREAM, Events),
    ok.

%% Extract the n field from an event's data — a convenient
%% identity for asserting which events came back.
ns(Events) ->
    [maps:get(n, E#evoq_event.data) || E <- Events].

versions(Events) ->
    [E#evoq_event.version || E <- Events].

%%====================================================================
%% Stream-not-found
%%====================================================================

read_from_nonexistent_stream_returns_stream_not_found_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual(
            {error, {stream_not_found, <<"missing$1">>}},
            mem_evoq_adapter:read(StoreId, <<"missing$1">>, 0, 10, forward))
    end).

%%====================================================================
%% Forward reads
%%====================================================================

forward_read_from_zero_returns_all_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward),
        ?assertEqual([1,2,3,4,5], ns(Events)),
        ?assertEqual([0,1,2,3,4], versions(Events))
    end).

forward_read_count_bounded_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 3, forward),
        ?assertEqual([1,2,3], ns(Events))
    end).

forward_read_from_middle_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 2, 10, forward),
        ?assertEqual([3,4,5], ns(Events))
    end).

forward_read_beyond_end_returns_partial_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 3),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 2, 10, forward),
        %% Stream has versions 0..2; requesting from version 2 with count 10
        %% returns just version 2 (no error, no padding).
        ?assertEqual([3], ns(Events))
    end).

forward_read_past_end_returns_empty_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 3),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 10, 5, forward),
        ?assertEqual([], Events)
    end).

%%====================================================================
%% Backward reads
%%====================================================================

backward_read_from_end_returns_descending_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 4, 10, backward),
        %% Versions 4,3,2,1,0 → data n values 5,4,3,2,1
        ?assertEqual([5,4,3,2,1], ns(Events)),
        ?assertEqual([4,3,2,1,0], versions(Events))
    end).

backward_read_count_bounded_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 4, 3, backward),
        %% Last 3 events, newest first: versions 4,3,2 → n=5,4,3
        ?assertEqual([5,4,3], ns(Events)),
        ?assertEqual([4,3,2], versions(Events))
    end).

backward_read_from_middle_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 2, 10, backward),
        %% From version 2, going backward: versions 2,1,0
        ?assertEqual([3,2,1], ns(Events))
    end).

backward_read_clamps_to_zero_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 3),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 1, 10, backward),
        %% From version 1 going back 10: clamps to version 0; returns [1, 0].
        ?assertEqual([2,1], ns(Events)),
        ?assertEqual([1,0], versions(Events))
    end).

backward_read_count_of_one_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, Events} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 2, 1, backward),
        ?assertEqual([3], ns(Events)),
        ?assertEqual([2], versions(Events))
    end).

%%====================================================================
%% Empty + edge
%%====================================================================

read_from_empty_stream_returns_empty_test() ->
    %% Append zero events to a stream (still creates the entry via the
    %% append path? No — empty Events list is a no-op. So this case
    %% exercises read of a stream the application is aware of but
    %% which has no events. Since we never created the entry, we
    %% expect stream_not_found.
    with_store(fun(StoreId) ->
        ?assertEqual(
            {error, {stream_not_found, <<"empty$1">>}},
            mem_evoq_adapter:read(StoreId, <<"empty$1">>, 0, 10, forward))
    end).

read_single_event_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 1),
        {ok, [E]} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 1, forward),
        ?assertEqual(0, E#evoq_event.version),
        ?assertEqual(<<"s$1">>, E#evoq_event.stream_id)
    end).

%%====================================================================
%% Multi-stream isolation
%%====================================================================

read_isolates_streams_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"a$1">>, 3),
        seed(StoreId, <<"b$1">>, 5),
        {ok, A} = mem_evoq_adapter:read(StoreId, <<"a$1">>, 0, 10, forward),
        {ok, B} = mem_evoq_adapter:read(StoreId, <<"b$1">>, 0, 10, forward),
        ?assertEqual(3, length(A)),
        ?assertEqual(5, length(B)),
        %% Cross-check: no event from b should appear in a's result.
        ?assert(lists:all(fun(E) -> E#evoq_event.stream_id =:= <<"a$1">> end, A)),
        ?assert(lists:all(fun(E) -> E#evoq_event.stream_id =:= <<"b$1">> end, B))
    end).

%%====================================================================
%% Append → read roundtrip with explicit fields
%%====================================================================

append_read_roundtrip_preserves_fields_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"s$1">>, ?NO_STREAM,
            [#{event_type => <<"foo_v1">>,
               data => #{value => 42},
               metadata => #{trace_id => <<"t-1">>},
               tags => [<<"realm:test">>]}]),
        {ok, [E]} = mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 1, forward),
        ?assertEqual(<<"foo_v1">>, E#evoq_event.event_type),
        ?assertEqual(#{value => 42}, E#evoq_event.data),
        ?assertEqual(#{trace_id => <<"t-1">>}, E#evoq_event.metadata),
        ?assertEqual([<<"realm:test">>], E#evoq_event.tags)
    end).
