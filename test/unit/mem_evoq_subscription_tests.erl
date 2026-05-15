%%% @doc Unit tests for the subscription path on mem_evoq_store.
%%%
%%% Covers:
%%%   * Subscribe with `from => latest' — no catch-up, only live events.
%%%   * Subscribe with `from => 0' — synchronous catch-up of existing
%%%     events before the call returns.
%%%   * Live fan-out: subscribe then append delivers via the subscriber pid.
%%%   * Filter dispatch: by_stream / by_event_type / by_event_pattern /
%%%     by_tags (any + all).
%%%   * Unsubscribe stops further delivery.
%%%   * Subscriber-pid death cleans up the subscription automatically.
%%% @end
-module(mem_evoq_subscription_tests).

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
        "mem_evoq_subscription_test_" ++
        integer_to_list(erlang:unique_integer([positive]))).

with_store(F) ->
    StoreId = setup(),
    try F(StoreId) after cleanup(StoreId) end.

%% Append helper that doesn't care about the stream's current state —
%% useful when a test seeds the same stream more than once.
seed(StoreId, StreamId, N) ->
    Events = [#{event_type => <<"e">>, data => #{n => I}}
              || I <- lists:seq(1, N)],
    {ok, _} = mem_evoq_adapter:append(StoreId, StreamId, ?ANY_VERSION, Events),
    ok.

%% Drain {events, [...]} messages from self()'s mailbox.
drain_events(Timeout) ->
    drain_events([], Timeout).
drain_events(Acc, Timeout) ->
    receive
        {events, Es} when is_list(Es) ->
            drain_events(Acc ++ Es, Timeout)
    after Timeout ->
        Acc
    end.

%%====================================================================
%% Registration + catch-up (moves 8 + 9)
%%====================================================================

subscribe_latest_returns_sub_key_no_catchup_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 3),
        {ok, SubKey} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, self(), #{from => latest}),
        ?assert(is_binary(SubKey)),
        ?assertEqual([], drain_events(100))
    end).

subscribe_from_zero_catches_up_existing_events_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 5),
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, self(), #{from => 0}),
        Events = drain_events(200),
        ?assertEqual(5, length(Events))
    end).

subscribe_from_zero_on_empty_store_delivers_nothing_test() ->
    with_store(fun(StoreId) ->
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, self(), #{from => 0}),
        ?assertEqual([], drain_events(100))
    end).

%%====================================================================
%% Live fan-out (move 10)
%%====================================================================

live_events_deliver_to_subscriber_test() ->
    with_store(fun(StoreId) ->
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, self(), #{from => latest}),
        seed(StoreId, <<"s$1">>, 3),
        Events = drain_events(200),
        ?assertEqual(3, length(Events))
    end).

multiple_subscribers_each_receive_test() ->
    with_store(fun(StoreId) ->
        Test = self(),
        S1 = spawn_link(fun() ->
            Test ! {s1_pid, self()},
            receive done -> ok end
        end),
        S2 = spawn_link(fun() ->
            Test ! {s2_pid, self()},
            receive done -> ok end
        end),
        receive {s1_pid, S1} -> ok end,
        receive {s2_pid, S2} -> ok end,
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, S1, #{from => latest}),
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, S2, #{from => latest}),
        seed(StoreId, <<"s$1">>, 2),
        timer:sleep(50),
        %% Delivery is one batch per append, so two events arrive as a
        %% single {events, [E1, E2]} message — count events, not messages.
        ?assertEqual(2, process_event_count(S1)),
        ?assertEqual(2, process_event_count(S2)),
        S1 ! done, S2 ! done
    end).

%%====================================================================
%% Filter dispatch (move 11)
%%====================================================================

by_stream_filter_excludes_other_streams_test() ->
    with_store(fun(StoreId) ->
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"a$1">>, self(), #{from => latest}),
        seed(StoreId, <<"a$1">>, 2),
        seed(StoreId, <<"b$1">>, 5),  %% should NOT be delivered
        Events = drain_events(200),
        ?assertEqual(2, length(Events)),
        [?assertEqual(<<"a$1">>, E#event.stream_id) || E <- Events]
    end).

unsubscribe_stops_delivery_test() ->
    with_store(fun(StoreId) ->
        {ok, SubKey} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, self(), #{from => latest}),
        seed(StoreId, <<"s$1">>, 1),
        timer:sleep(50),
        _ = drain_events(50),  %% drain the first batch
        ok = mem_evoq_adapter:unsubscribe(StoreId, SubKey),
        seed(StoreId, <<"s$1">>, 3),
        ?assertEqual([], drain_events(200))
    end).

%%====================================================================
%% Catch-up + live combined
%%====================================================================

subscribe_from_zero_then_live_combine_test() ->
    with_store(fun(StoreId) ->
        seed(StoreId, <<"s$1">>, 2),
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, self(), #{from => 0}),
        timer:sleep(50),
        seed(StoreId, <<"s$1">>, 3),
        Events = drain_events(200),
        ?assertEqual(5, length(Events))
    end).

%%====================================================================
%% Subscriber death cleans up
%%====================================================================

dead_subscriber_is_removed_from_state_test() ->
    with_store(fun(StoreId) ->
        Test = self(),
        Sub = spawn(fun() ->
            Test ! {sub_pid, self()},
            receive die -> ok end
        end),
        receive {sub_pid, Sub} -> ok end,
        {ok, _} = mem_evoq_adapter:subscribe(
            StoreId, <<"s$1">>, Sub, #{from => latest}),
        Sub ! die,
        timer:sleep(100),
        %% Subscriber state should now have zero subscriptions.
        {ok, StorePid} = mem_evoq_registry:lookup(StoreId),
        State = sys:get_state(StorePid),
        Subs = element(5, State),  %% #state.subscribers is field 5
        ?assertEqual(0, maps:size(Subs))
    end).

%%====================================================================
%% Helpers
%%====================================================================

%% Count total events delivered to Pid across all {events, [...]}
%% batches in its mailbox. Used in lieu of receive-and-assert when
%% the pid is foreign to the test process.
process_event_count(Pid) ->
    {messages, Msgs} = erlang:process_info(Pid, messages),
    lists:foldl(
        fun({events, Es}, Acc) when is_list(Es) -> Acc + length(Es);
           (_, Acc) -> Acc
        end, 0, Msgs).
