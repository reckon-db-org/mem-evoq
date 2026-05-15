%%% @doc PropEr suite for the mem-evoq adapter.
%%%
%%% Pure properties over fresh stores. Each property:
%%%
%%%   1. Generates an event-count and (where relevant) a fresh HMAC key.
%%%   2. Starts a clean store, drives it, then stops it.
%%%   3. Asserts the property holds.
%%%
%%% Move 19 of the mem-evoq plan.
%%% @end
-module(prop_mem_evoq).

-include_lib("proper/include/proper.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").
-include_lib("evoq/include/evoq_types.hrl").

%%====================================================================
%% Generators
%%====================================================================

event_count() ->
    %% 1..20 keeps the shrunk counterexample small and the test fast.
    integer(1, 20).

key() ->
    binary(32).

stream_id() ->
    ?LET(N, integer(0, 1000), iolist_to_binary([<<"stream$">>, integer_to_binary(N)])).

%%====================================================================
%% Properties — plain store
%%====================================================================

%% Appending N events into a fresh stream makes version(StreamId) =:= N-1,
%% and reading 0..N-1 returns N events with versions [0..N-1] in order.
prop_append_read_roundtrip() ->
    ?FORALL({N, StreamId}, {event_count(), stream_id()},
        with_store(disabled, fun(StoreId) ->
            seed(StoreId, StreamId, N),
            VersionOk = (mem_evoq_adapter:version(StoreId, StreamId) =:= N - 1),
            {ok, Events} = mem_evoq_adapter:read(StoreId, StreamId, 0, N + 5, forward),
            length(Events) =:= N
                andalso versions_are(Events, lists:seq(0, N - 1))
                andalso VersionOk
        end)).

%% A saved snapshot loads back with the same data and version.
prop_snapshot_roundtrip() ->
    ?FORALL({StreamId, V}, {stream_id(), integer(0, 100)},
        with_store(disabled, fun(StoreId) ->
            Data = #{value => V, marker => <<"snap">>},
            ok = mem_evoq_adapter:save_snapshot(StoreId, StreamId, V, Data, #{}),
            {ok, Loaded} = mem_evoq_adapter:load_snapshot(StoreId, StreamId),
            Loaded#snapshot.version =:= V andalso Loaded#snapshot.data =:= Data
        end)).

%%====================================================================
%% Properties — integrity-enabled store
%%====================================================================

%% Strict read of an untampered integrity-enabled stream returns the
%% same N events as a plain read.
prop_strict_read_round_trip() ->
    ?FORALL({N, StreamId, Key}, {event_count(), stream_id(), key()},
        with_integrity_store(Key, fun(StoreId) ->
            seed(StoreId, StreamId, N),
            {ok, Plain} = mem_evoq_adapter:read(
                StoreId, StreamId, 0, N + 5, forward),
            {ok, Strict} = mem_evoq_adapter:read(
                StoreId, StreamId, 0, N + 5, forward, #{verify => strict}),
            event_ids(Plain) =:= event_ids(Strict)
        end)).

%% Backward strict read returns the same N events as the forward read,
%% just in reverse version order. The "reverse → verify forward →
%% reverse back" trick from reckon-db 2.1.1 must round-trip.
prop_backward_strict_matches_forward() ->
    ?FORALL({N, StreamId, Key}, {event_count(), stream_id(), key()},
        with_integrity_store(Key, fun(StoreId) ->
            seed(StoreId, StreamId, N),
            {ok, Forward} = mem_evoq_adapter:read(
                StoreId, StreamId, 0, N + 5, forward, #{verify => strict}),
            {ok, Backward} = mem_evoq_adapter:read(
                StoreId, StreamId, N - 1, N + 5, backward, #{verify => strict}),
            event_ids(Forward) =:= lists:reverse(event_ids(Backward))
        end)).

%% Every appended event's prev_event_hash equals the chain hash of
%% its predecessor (or genesis for event 0). Walked against raw
%% #event{} records because compute_chain_hash needs the storage
%% record (mac + signature carry signal the adapter strips on the
%% way out).
prop_chain_continuity() ->
    ?FORALL({N, StreamId, Key}, {event_count(), stream_id(), key()},
        with_integrity_store(Key, fun(StoreId) ->
            seed(StoreId, StreamId, N),
            Events = raw_events(StoreId, StreamId),
            Genesis = reckon_gater_integrity:genesis_prev_hash(),
            walk_chain(Events, Genesis)
        end)).

%% Snapshot saved against an integrity-enabled store: load returns the
%% same data + version + a populated anchor_hash + a populated mac.
prop_integrity_snapshot_roundtrip() ->
    ?FORALL({N, StreamId, Key}, {event_count(), stream_id(), key()},
        with_integrity_store(Key, fun(StoreId) ->
            seed(StoreId, StreamId, N),
            SnapVersion = N - 1,
            Data = #{state => running, n => N},
            ok = mem_evoq_adapter:save_snapshot(
                StoreId, StreamId, SnapVersion, Data, #{}),
            {ok, Loaded} = mem_evoq_adapter:load_snapshot(StoreId, StreamId),
            Loaded#snapshot.version =:= SnapVersion
                andalso Loaded#snapshot.data =:= Data
                andalso is_binary(Loaded#snapshot.anchor_hash)
                andalso byte_size(Loaded#snapshot.anchor_hash) =:= 32
                andalso is_tuple(Loaded#snapshot.mac)
        end)).

%%====================================================================
%% Helpers
%%====================================================================

with_store(disabled, Body) ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    StoreId = unique_store_id("prop"),
    {ok, _} = mem_evoq:start_store(StoreId),
    try Body(StoreId) after catch mem_evoq:stop_store(StoreId) end.

with_integrity_store(Key, Body) ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    StoreId = unique_store_id("prop_int"),
    {ok, _} = mem_evoq:start_store(
        StoreId, #{integrity => #{enabled => true, key => Key}}),
    try Body(StoreId) after catch mem_evoq:stop_store(StoreId) end.

unique_store_id(Tag) ->
    list_to_atom(
        Tag ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))).

seed(StoreId, StreamId, N) ->
    Events = [#{event_type => <<"e">>, data => #{n => I}}
              || I <- lists:seq(1, N)],
    {ok, _} = mem_evoq_adapter:append(StoreId, StreamId, ?ANY_VERSION, Events),
    ok.

versions_are(Events, Expected) ->
    [E#evoq_event.version || E <- Events] =:= Expected.

event_ids(Events) ->
    [E#evoq_event.event_id || E <- Events].

%% Raw #event{} records straight out of the store's state — used by
%% chain-walk properties that need the storage-side fields.
raw_events(StoreId, StreamId) ->
    {ok, Pid} = mem_evoq_registry:lookup(StoreId),
    State = sys:get_state(Pid),
    Streams = element(3, State),
    maps:get(StreamId, Streams, []).

walk_chain([], _Prev) ->
    true;
walk_chain([#event{prev_event_hash = Prev} = E | Rest], Prev) ->
    Next = reckon_gater_integrity:compute_chain_hash(E, Prev),
    walk_chain(Rest, Next);
walk_chain(_, _) ->
    false.
