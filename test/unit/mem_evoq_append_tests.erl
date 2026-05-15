%%% @doc Unit tests for the append/4 path on mem_evoq_store.
%%%
%%% Mirrors the semantic surface of reckon_db_streams:append/4 since
%%% mem-evoq's contract is that it behaves identically to a single-
%%% node integrity-disabled reckon-db (the integrity-enabled flavour
%%% lands later in the move sequence).
%%% @end
-module(mem_evoq_append_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").
%% ?NO_STREAM, ?ANY_VERSION, ?STREAM_EXISTS come from the header above.

%%====================================================================
%% Fixture
%%====================================================================

setup() ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    StoreId = unique_store_id(),
    {ok, _Pid} = mem_evoq:start_store(StoreId),
    StoreId.

cleanup(StoreId) ->
    ok = mem_evoq:stop_store(StoreId).

unique_store_id() ->
    list_to_atom(
        "mem_evoq_append_test_" ++
        integer_to_list(erlang:unique_integer([positive]))).

with_store(F) ->
    StoreId = setup(),
    try F(StoreId) after cleanup(StoreId) end.

ev(Type, Data) ->
    #{event_type => Type, data => Data}.

%%====================================================================
%% Append on fresh stream
%%====================================================================

append_one_to_new_stream_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM,
            [ev(<<"created_v1">>, #{n => 1})])
    end).

append_multiple_to_new_stream_assigns_sequential_versions_test() ->
    with_store(fun(StoreId) ->
        {ok, 2} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM,
            [ev(<<"e">>, #{}), ev(<<"e">>, #{}), ev(<<"e">>, #{})])
    end).

%%====================================================================
%% Append to existing stream
%%====================================================================

append_to_existing_stream_advances_version_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM, [ev(<<"e">>, #{n => 1})]),
        {ok, 1} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, 0, [ev(<<"e">>, #{n => 2})])
    end).

append_batch_to_existing_stream_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM, [ev(<<"e">>, #{})]),
        {ok, 3} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, 0,
            [ev(<<"e">>, #{}), ev(<<"e">>, #{}), ev(<<"e">>, #{})])
    end).

%%====================================================================
%% Expected-version sentinels
%%====================================================================

any_version_accepts_any_state_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?ANY_VERSION, [ev(<<"e">>, #{})]),
        {ok, 1} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?ANY_VERSION, [ev(<<"e">>, #{})])
    end).

stream_exists_rejects_new_stream_test() ->
    with_store(fun(StoreId) ->
        Result = mem_evoq_adapter:append(
            StoreId, <<"acct$new">>, ?STREAM_EXISTS, [ev(<<"e">>, #{})]),
        ?assertMatch({error, {wrong_expected_version, ?STREAM_EXISTS, ?NO_STREAM}},
                     Result)
    end).

stream_exists_accepts_existing_stream_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM, [ev(<<"e">>, #{})]),
        {ok, 1} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?STREAM_EXISTS, [ev(<<"e">>, #{})])
    end).

no_stream_on_existing_stream_is_rejected_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM, [ev(<<"e">>, #{})]),
        Result = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM, [ev(<<"e">>, #{})]),
        ?assertMatch({error, {wrong_expected_version, ?NO_STREAM, 0}}, Result)
    end).

%%====================================================================
%% Optimistic concurrency
%%====================================================================

mismatched_expected_version_is_rejected_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM, [ev(<<"e">>, #{})]),
        %% Expected 5, actual 0 — should fail.
        Result = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, 5, [ev(<<"e">>, #{})]),
        ?assertMatch({error, {wrong_expected_version, 5, 0}}, Result)
    end).

%%====================================================================
%% Event record shape
%%====================================================================

%% The state isn't directly inspectable from the adapter — that's
%% fine; later moves will add read/5 which lets us assert event
%% shape via the public API. For now we test the shape via a
%% sys:get_state on the store gen_server.
event_record_carries_expected_fields_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM,
            [#{event_type => <<"created_v1">>,
               data => #{name => <<"Alice">>},
               metadata => #{request_id => <<"req-1">>},
               tags => [<<"realm:io.macula">>]}]),
        {ok, Pid} = mem_evoq_registry:lookup(StoreId),
        State = sys:get_state(Pid),
        Streams = element(3, State), %% #state.streams is field 3
        [#event{} = E] = maps:get(<<"acct$1">>, Streams),
        ?assertEqual(<<"created_v1">>, E#event.event_type),
        ?assertEqual(<<"acct$1">>, E#event.stream_id),
        ?assertEqual(0, E#event.version),
        ?assertEqual(#{name => <<"Alice">>}, E#event.data),
        ?assertEqual(#{request_id => <<"req-1">>}, E#event.metadata),
        ?assertEqual([<<"realm:io.macula">>], E#event.tags),
        ?assertEqual(<<"application/json">>, E#event.data_content_type),
        %% Timestamp + epoch_us populated automatically.
        ?assert(is_integer(E#event.timestamp)),
        ?assert(is_integer(E#event.epoch_us)),
        %% Integrity fields untouched — populated in a later move.
        ?assertEqual(undefined, E#event.prev_event_hash),
        ?assertEqual(undefined, E#event.mac)
    end).

event_id_is_generated_when_absent_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM, [ev(<<"e">>, #{})]),
        {ok, Pid} = mem_evoq_registry:lookup(StoreId),
        State = sys:get_state(Pid),
        Streams = element(3, State),
        [E] = maps:get(<<"acct$1">>, Streams),
        ?assert(is_binary(E#event.event_id)),
        ?assertEqual(32, byte_size(E#event.event_id))  %% 16 bytes hex-encoded
    end).

event_id_is_preserved_when_supplied_test() ->
    with_store(fun(StoreId) ->
        SuppliedId = <<"my-explicit-id">>,
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"acct$1">>, ?NO_STREAM,
            [#{event_type => <<"e">>, data => #{}, event_id => SuppliedId}]),
        {ok, Pid} = mem_evoq_registry:lookup(StoreId),
        State = sys:get_state(Pid),
        Streams = element(3, State),
        [E] = maps:get(<<"acct$1">>, Streams),
        ?assertEqual(SuppliedId, E#event.event_id)
    end).

%%====================================================================
%% Multiple streams
%%====================================================================

multiple_streams_are_independent_test() ->
    with_store(fun(StoreId) ->
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"a$1">>, ?NO_STREAM, [ev(<<"e">>, #{})]),
        {ok, 0} = mem_evoq_adapter:append(
            StoreId, <<"b$1">>, ?NO_STREAM, [ev(<<"e">>, #{})]),
        %% Independent version counters
        {ok, 1} = mem_evoq_adapter:append(
            StoreId, <<"a$1">>, 0, [ev(<<"e">>, #{})]),
        {ok, 1} = mem_evoq_adapter:append(
            StoreId, <<"b$1">>, 0, [ev(<<"e">>, #{})])
    end).
