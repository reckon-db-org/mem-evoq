%%% @doc Unit tests for stream-metadata operations on mem_evoq_store:
%%% version/2, exists/2, has_events/1, list_streams/1, delete/2.
%%%
%%% Mirrors the semantic surface of the reckon_db_streams equivalents
%%% so consumers can swap adapters without behavioural surprise.
%%% @end
-module(mem_evoq_metadata_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").

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
        "mem_evoq_metadata_test_" ++
        integer_to_list(erlang:unique_integer([positive]))).

with_store(F) ->
    StoreId = setup(),
    try F(StoreId) after cleanup(StoreId) end.

seed(StoreId, StreamId, N) ->
    Events = [#{event_type => <<"e">>, data => #{n => I}}
              || I <- lists:seq(1, N)],
    {ok, _} = mem_evoq_adapter:append(StoreId, StreamId, ?NO_STREAM, Events),
    ok.

%%====================================================================
%% version/2
%%====================================================================

version_of_missing_stream_is_no_stream_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual(?NO_STREAM,
                     mem_evoq_adapter:version(StoreId, <<"missing$1">>))
    end).

version_after_single_append_is_zero_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 1),
        ?assertEqual(0, mem_evoq_adapter:version(StoreId, <<"s$1">>))
    end).

version_advances_with_appends_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        ?assertEqual(4, mem_evoq_adapter:version(StoreId, <<"s$1">>))
    end).

%%====================================================================
%% exists/2
%%====================================================================

exists_returns_false_for_missing_stream_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual(false, mem_evoq_adapter:exists(StoreId, <<"missing$1">>))
    end).

exists_returns_true_after_append_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 1),
        ?assertEqual(true, mem_evoq_adapter:exists(StoreId, <<"s$1">>))
    end).

empty_event_list_append_does_not_create_a_stream_test() ->
    %% Mirrors reckon-db: appending zero events to a non-existent
    %% stream is a no-op, does NOT create an empty stream entry.
    with_store(fun(StoreId) ->
        {ok, ?NO_STREAM} = mem_evoq_adapter:append(
            StoreId, <<"phantom$1">>, ?ANY_VERSION, []),
        ?assertEqual(false, mem_evoq_adapter:exists(StoreId, <<"phantom$1">>))
    end).

%%====================================================================
%% has_events/1
%%====================================================================

has_events_returns_false_on_empty_store_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual(false, mem_evoq_adapter:has_events(StoreId))
    end).

has_events_returns_true_after_first_append_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 1),
        ?assertEqual(true, mem_evoq_adapter:has_events(StoreId))
    end).

has_events_after_delete_of_only_stream_returns_false_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 1),
        ?assertEqual(true, mem_evoq_adapter:has_events(StoreId)),
        ok = mem_evoq_adapter:delete_stream(StoreId, <<"s$1">>),
        ?assertEqual(false, mem_evoq_adapter:has_events(StoreId))
    end).

%%====================================================================
%% list_streams/1
%%====================================================================

list_streams_on_empty_store_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual({ok, []}, mem_evoq_adapter:list_streams(StoreId))
    end).

list_streams_returns_all_stream_ids_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"a$1">>, 1),
        seed(StoreId, <<"b$2">>, 1),
        seed(StoreId, <<"c$3">>, 1),
        {ok, Streams} = mem_evoq_adapter:list_streams(StoreId),
        ?assertEqual([<<"a$1">>, <<"b$2">>, <<"c$3">>], lists:sort(Streams))
    end).

list_streams_after_delete_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"a$1">>, 1),
        seed(StoreId, <<"b$1">>, 1),
        ok = mem_evoq_adapter:delete_stream(StoreId, <<"a$1">>),
        {ok, Streams} = mem_evoq_adapter:list_streams(StoreId),
        ?assertEqual([<<"b$1">>], Streams)
    end).

%%====================================================================
%% delete/2
%%====================================================================

delete_removes_stream_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 3),
        ?assertEqual(true, mem_evoq_adapter:exists(StoreId, <<"s$1">>)),
        ok = mem_evoq_adapter:delete_stream(StoreId, <<"s$1">>),
        ?assertEqual(false, mem_evoq_adapter:exists(StoreId, <<"s$1">>))
    end).

delete_makes_read_return_stream_not_found_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 3),
        ok = mem_evoq_adapter:delete_stream(StoreId, <<"s$1">>),
        ?assertEqual({error, {stream_not_found, <<"s$1">>}},
                     mem_evoq_adapter:read(StoreId, <<"s$1">>, 0, 10, forward))
    end).

delete_is_idempotent_for_missing_stream_test() ->
    with_store(fun(StoreId) ->
        ?assertEqual(ok, mem_evoq_adapter:delete_stream(StoreId, <<"never_existed$1">>)),
        ?assertEqual(ok, mem_evoq_adapter:delete_stream(StoreId, <<"never_existed$1">>))
    end).

delete_does_not_affect_other_streams_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"a$1">>, 3),
        seed(StoreId, <<"b$1">>, 3),
        ok = mem_evoq_adapter:delete_stream(StoreId, <<"a$1">>),
        ?assertEqual(false, mem_evoq_adapter:exists(StoreId, <<"a$1">>)),
        ?assertEqual(true,  mem_evoq_adapter:exists(StoreId, <<"b$1">>)),
        ?assertEqual(2,     mem_evoq_adapter:version(StoreId, <<"b$1">>))
    end).
