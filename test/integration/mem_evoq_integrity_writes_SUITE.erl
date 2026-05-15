%% @doc Common Test suite mirroring reckon_db_integrity_writes_SUITE
%% against the mem-evoq adapter.
%%
%% Same property assertions: events appended through a store with
%% integrity enabled carry prev_event_hash + mac populated correctly,
%% the chain is continuous across batched appends, disabled stores
%% produce no integrity fields, and the key actually flows through to
%% the computed MAC.
%%
%% Two record types in play:
%%
%%   * `#event{}'      — storage-internal (reckon_gater). Has mac +
%%                       signature. Reached via sys:get_state.
%%   * `#evoq_event{}' — what the adapter returns. mac + signature are
%%                       intentionally NOT propagated (storage layer).
%%
%% Move 18 of the mem-evoq plan: the round-trip integration test.
%% @end
-module(mem_evoq_integrity_writes_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").
-include_lib("evoq/include/evoq_types.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([disabled_store_writes_legacy_events/1,
         enabled_store_writes_integrity_fields/1,
         chain_continues_across_appends/1,
         watermark_is_recorded_on_first_append/1,
         different_keys_produce_different_macs/1,
         read_verifier_walks_full_chain/1]).

%%====================================================================
%% CT boilerplate
%%====================================================================

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [disabled_store_writes_legacy_events,
     enabled_store_writes_integrity_fields,
     chain_continues_across_appends,
     watermark_is_recorded_on_first_append,
     different_keys_produce_different_macs,
     read_verifier_walks_full_chain].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(mem_evoq),
    Config.

end_per_suite(_Config) -> ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    StoreId = list_to_atom(
        "mem_evoq_integrity_writes_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),
    [{store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = ?config(store_id, Config),
    catch mem_evoq:stop_store(StoreId),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% Disabled store (default) writes events with no integrity fields.
disabled_store_writes_legacy_events(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    StreamId = <<"stream-disabled">>,
    {ok, 0} = mem_evoq_adapter:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"x_happened">>, data => #{n => 1}}]),

    %% Adapter view: prev_event_hash propagated, undefined for legacy.
    {ok, [EvoqEvent]} = mem_evoq_adapter:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(undefined, EvoqEvent#evoq_event.prev_event_hash),

    %% mac + signature live on the storage-side #event{} record only.
    [Raw] = raw_events(StoreId, StreamId),
    ?assertEqual(undefined, Raw#event.mac),
    ?assertEqual(undefined, Raw#event.signature),
    ok.

%% First event in an integrity-enabled store: genesis prev_event_hash,
%% {1, 32-byte} mac, verifier accepts under the loaded key.
enabled_store_writes_integrity_fields(Config) ->
    {StoreId, Key} = setup_integrity_store(Config),

    StreamId = <<"stream-enabled-0">>,
    {ok, 0} = mem_evoq_adapter:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"x_happened">>, data => #{n => 1}}]),

    {ok, [EvoqEvent]} = mem_evoq_adapter:read(StoreId, StreamId, 0, 10, forward),

    Genesis = reckon_gater_integrity:genesis_prev_hash(),
    ?assertEqual(Genesis, EvoqEvent#evoq_event.prev_event_hash),

    %% MAC + verifier acceptance live on the raw storage record.
    [Raw] = raw_events(StoreId, StreamId),
    ?assertMatch({1, _}, Raw#event.mac),
    {1, MacBytes} = Raw#event.mac,
    ?assertEqual(32, byte_size(MacBytes)),
    ?assertEqual(ok,
        reckon_gater_integrity:verify_event(Raw, Genesis, Key)),

    ?assertEqual(0, lookup_chain_start(StoreId, StreamId)),
    ok.

%% Across batched appends, each event's prev_event_hash equals the
%% chain hash of its predecessor.
chain_continues_across_appends(Config) ->
    {StoreId, Key} = setup_integrity_store(Config),
    StreamId = <<"stream-chain-test">>,

    {ok, 1} = mem_evoq_adapter:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e1">>, data => #{n => 1}},
         #{event_type => <<"e2">>, data => #{n => 2}}]),
    {ok, 4} = mem_evoq_adapter:append(
        StoreId, StreamId, 1,
        [#{event_type => <<"e3">>, data => #{n => 3}},
         #{event_type => <<"e4">>, data => #{n => 4}},
         #{event_type => <<"e5">>, data => #{n => 5}}]),

    %% Walk via raw storage records — compute_chain_hash takes #event{}.
    Events = raw_events(StoreId, StreamId),
    ?assertEqual(5, length(Events)),
    Genesis = reckon_gater_integrity:genesis_prev_hash(),
    walk_chain_and_verify(Events, Genesis, Key),
    ok.

%% First append on each stream records the chain-start watermark.
%% Subsequent appends on the same stream don't move it.
watermark_is_recorded_on_first_append(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),

    StreamA = <<"stream-A">>,
    StreamB = <<"stream-B">>,

    ?assertEqual(undefined, lookup_chain_start(StoreId, StreamA)),
    ?assertEqual(undefined, lookup_chain_start(StoreId, StreamB)),

    {ok, 0} = mem_evoq_adapter:append(
        StoreId, StreamA, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{}}]),

    ?assertEqual(0, lookup_chain_start(StoreId, StreamA)),
    ?assertEqual(undefined, lookup_chain_start(StoreId, StreamB)),

    {ok, 1} = mem_evoq_adapter:append(
        StoreId, StreamA, 0,
        [#{event_type => <<"e">>, data => #{}}]),
    ?assertEqual(0, lookup_chain_start(StoreId, StreamA)),
    ok.

%% Same payload, different keys → different MACs. Inspect via raw
%% storage records.
different_keys_produce_different_macs(Config) ->
    Store1 = ?config(store_id, Config),
    Store2 = list_to_atom(atom_to_list(Store1) ++ "_alt"),

    Key1 = crypto:strong_rand_bytes(32),
    Key2 = crypto:strong_rand_bytes(32),
    {ok, _} = mem_evoq:start_store(Store1, #{integrity => #{enabled => true, key => Key1}}),
    {ok, _} = mem_evoq:start_store(Store2, #{integrity => #{enabled => true, key => Key2}}),

    StreamId = <<"stream-mac-vs">>,
    EventPayload = #{event_type => <<"e">>, data => #{value => 42}},

    {ok, 0} = mem_evoq_adapter:append(Store1, StreamId, ?NO_STREAM, [EventPayload]),
    [#event{mac = {_, Mac1}}] = raw_events(Store1, StreamId),

    {ok, 0} = mem_evoq_adapter:append(Store2, StreamId, ?NO_STREAM, [EventPayload]),
    [#event{mac = {_, Mac2}}] = raw_events(Store2, StreamId),

    ?assertNotEqual(Mac1, Mac2),

    catch mem_evoq:stop_store(Store2),
    ok.

%% End-to-end round-trip: append → read with verify=strict reads back
%% all events with no integrity_violation, proving the read-path
%% verifier walks the full chain successfully on untampered streams.
read_verifier_walks_full_chain(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"stream-roundtrip">>,

    Payloads = [#{event_type => <<"e">>, data => #{n => I}}
                || I <- lists:seq(1, 8)],
    {ok, 7} = mem_evoq_adapter:append(StoreId, StreamId, ?NO_STREAM, Payloads),

    {ok, Forward} = mem_evoq_adapter:read(
        StoreId, StreamId, 0, 20, forward, #{verify => strict}),
    ?assertEqual(8, length(Forward)),

    {ok, Backward} = mem_evoq_adapter:read(
        StoreId, StreamId, 7, 20, backward, #{verify => strict}),
    ?assertEqual(8, length(Backward)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

setup_integrity_store(Config) ->
    StoreId = ?config(store_id, Config),
    Key = crypto:strong_rand_bytes(32),
    {ok, _} = mem_evoq:start_store(
        StoreId, #{integrity => #{enabled => true, key => Key}}),
    {StoreId, Key}.

%% Reach the raw #event{} records for a stream. The adapter strips
%% mac + signature; assertions on those properties have to bypass it.
raw_events(StoreId, StreamId) ->
    {ok, Pid} = mem_evoq_registry:lookup(StoreId),
    State = sys:get_state(Pid),
    Streams = element(3, State),
    maps:get(StreamId, Streams).

%% mem-evoq keeps chain-start watermarks inside the store gen_server's
%% state. The integration test peeks at them via sys:get_state — same
%% approach taken by mem_evoq_integrity_snapshot_tests.
lookup_chain_start(StoreId, StreamId) ->
    {ok, Pid} = mem_evoq_registry:lookup(StoreId),
    State = sys:get_state(Pid),
    Integrity = element(6, State),
    chain_start_in(Integrity, StreamId).

chain_start_in(disabled, _StreamId) ->
    undefined;
chain_start_in(#{chain_start := Map}, StreamId) ->
    maps:get(StreamId, Map, undefined).

walk_chain_and_verify([], _PrevTip, _Key) ->
    ok;
walk_chain_and_verify([Event | Rest], PrevTip, Key) ->
    ?assertEqual(ok,
        reckon_gater_integrity:verify_event(Event, PrevTip, Key)),
    NextTip = reckon_gater_integrity:compute_chain_hash(Event, PrevTip),
    walk_chain_and_verify(Rest, NextTip, Key).
