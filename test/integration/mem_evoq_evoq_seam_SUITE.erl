%% @doc Common Test suite proving the mem-evoq adapter works through
%% the real evoq_event_store dispatch.
%%
%% This is the test the 0.1.0 release lacked. mem_evoq_adapter:read/5
%% returned reckon_gater `#event{}' records; evoq_event_store:read/5
%% then called event_to_map/1 on each return value; event_to_map/1
%% only handles `#evoq_event{}' or maps, so the dispatch crashed with
%% function_clause whenever the adapter was actually plumbed into
%% evoq. The mem-evoq 0.1.0 test suite never crossed that seam — it
%% called mem_evoq_adapter directly.
%%
%% 0.1.1 added the boundary translation. This suite locks down the
%% fix: every test reads back through evoq_event_store rather than the
%% adapter, and asserts on the flat maps that evoq returns (the shape
%% aggregates and projections see).
%%
%% Move 24 of the mem-evoq plan: prove the adapter works against the
%% real evoq dispatch surface.
%% @end
-module(mem_evoq_evoq_seam_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([append_then_evoq_read_returns_flat_maps/1,
         evoq_read_preserves_envelope_fields/1,
         evoq_read_merges_payload_into_top_level/1,
         evoq_read_empty_stream_surfaces_error/1,
         evoq_read_of_integrity_enabled_carries_prev_event_hash/1,
         evoq_read_of_legacy_stream_has_undefined_prev_event_hash/1,
         evoq_snapshot_save_load_roundtrip/1,
         evoq_snapshot_load_returns_flat_map/1,
         evoq_snapshot_not_found_propagates/1]).

%%====================================================================
%% CT boilerplate
%%====================================================================

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [append_then_evoq_read_returns_flat_maps,
     evoq_read_preserves_envelope_fields,
     evoq_read_merges_payload_into_top_level,
     evoq_read_empty_stream_surfaces_error,
     evoq_read_of_integrity_enabled_carries_prev_event_hash,
     evoq_read_of_legacy_stream_has_undefined_prev_event_hash,
     evoq_snapshot_save_load_roundtrip,
     evoq_snapshot_load_returns_flat_map,
     evoq_snapshot_not_found_propagates].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(mem_evoq),
    %% Plumb the adapter into BOTH evoq seams for the suite.
    ok = evoq_event_store:set_adapter(mem_evoq_adapter),
    ok = evoq_snapshot_store:set_adapter(mem_evoq_adapter),
    Config.

end_per_suite(_Config) -> ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    StoreId = list_to_atom(
        "mem_evoq_seam_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),
    [{store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = ?config(store_id, Config),
    catch mem_evoq:stop_store(StoreId),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% The base case the 0.1.0 bug ate: read through evoq_event_store
%% must return a list of flat maps, not raw records, and must not
%% crash.
append_then_evoq_read_returns_flat_maps(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    StreamId = <<"agg-flat">>,
    {ok, 1} = mem_evoq_adapter:append(StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"x_happened_v1">>, data => #{count => 1}},
         #{event_type => <<"x_happened_v1">>, data => #{count => 2}}]),

    {ok, [E0, E1]} = evoq_event_store:read(StoreId, StreamId, 0, 10, forward),
    ?assert(is_map(E0)),
    ?assert(is_map(E1)),
    ?assertEqual(0, maps:get(version, E0)),
    ?assertEqual(1, maps:get(version, E1)),
    ok.

%% Envelope fields (event_id, event_type, stream_id, version,
%% metadata, timestamp, epoch_us) reach the consumer via the flat
%% map. event_to_map/1 is what does this — we're verifying mem-evoq
%% feeds it the right shape (a real `#evoq_event{}', not the raw
%% `#event{}' that 0.1.0 returned).
evoq_read_preserves_envelope_fields(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    {ok, 0} = mem_evoq_adapter:append(StoreId, <<"agg-env">>, ?NO_STREAM,
        [#{event_type => <<"user_registered_v1">>,
           data => #{user_id => <<"u-1">>, email => <<"a@b">>},
           metadata => #{trace_id => <<"t-1">>}}]),

    {ok, [Event]} = evoq_event_store:read(
        StoreId, <<"agg-env">>, 0, 10, forward),

    ?assertEqual(<<"user_registered_v1">>, maps:get(event_type, Event)),
    ?assertEqual(<<"agg-env">>, maps:get(stream_id, Event)),
    ?assertEqual(0, maps:get(version, Event)),
    ?assertEqual(#{trace_id => <<"t-1">>}, maps:get(metadata, Event)),
    ?assert(is_integer(maps:get(timestamp, Event))),
    ?assert(is_integer(maps:get(epoch_us, Event))),
    ?assert(is_binary(maps:get(event_id, Event))),
    ok.

%% evoq's contract: payload keys are merged into the top level of the
%% returned map (envelope wins on collision). Aggregate apply/2
%% callbacks rely on this.
evoq_read_merges_payload_into_top_level(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    {ok, 0} = mem_evoq_adapter:append(StoreId, <<"agg-merge">>, ?NO_STREAM,
        [#{event_type => <<"price_set_v1">>,
           data => #{sku => <<"sku-1">>, cents => 4200}}]),

    {ok, [Event]} = evoq_event_store:read(
        StoreId, <<"agg-merge">>, 0, 10, forward),

    %% Payload fields surface at top level alongside the envelope.
    ?assertEqual(<<"sku-1">>, maps:get(sku, Event)),
    ?assertEqual(4200, maps:get(cents, Event)),
    %% Envelope still there.
    ?assertEqual(<<"price_set_v1">>, maps:get(event_type, Event)),
    ok.

%% Empty / missing stream surfaces a structured error through evoq.
evoq_read_empty_stream_surfaces_error(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    ?assertMatch({error, {stream_not_found, _}},
        evoq_event_store:read(StoreId, <<"missing">>, 0, 10, forward)),
    ok.

%% Integrity-enabled stores expose the chain hash to evoq consumers
%% via the prev_event_hash field. This is the keyless defense-in-
%% depth signal projections and process managers verify against.
evoq_read_of_integrity_enabled_carries_prev_event_hash(Config) ->
    StoreId = ?config(store_id, Config),
    Key = crypto:strong_rand_bytes(32),
    {ok, _} = mem_evoq:start_store(
        StoreId, #{integrity => #{enabled => true, key => Key}}),

    {ok, 1} = mem_evoq_adapter:append(StoreId, <<"agg-int">>, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => 1}},
         #{event_type => <<"e">>, data => #{n => 2}}]),

    {ok, [E0, E1]} = evoq_event_store:read(
        StoreId, <<"agg-int">>, 0, 10, forward),

    Genesis = reckon_gater_integrity:genesis_prev_hash(),
    ?assertEqual(Genesis, maps:get(prev_event_hash, E0)),
    ?assert(is_binary(maps:get(prev_event_hash, E1))),
    ?assertNotEqual(Genesis, maps:get(prev_event_hash, E1)),
    ok.

%% Disabled (legacy) stores carry prev_event_hash=undefined to evoq.
evoq_read_of_legacy_stream_has_undefined_prev_event_hash(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    {ok, 0} = mem_evoq_adapter:append(StoreId, <<"agg-leg">>, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{}}]),

    {ok, [E]} = evoq_event_store:read(
        StoreId, <<"agg-leg">>, 0, 10, forward),

    ?assertEqual(undefined, maps:get(prev_event_hash, E)),
    ok.

%%--------------------------------------------------------------------
%% Snapshot seam — these tests exist to lock down the 0.1.2 fix.
%% Until 0.1.2, mem_evoq_adapter exposed `save_snapshot' / `load_snapshot'
%% which evoq_snapshot_store does not call. evoq cannot wire mem_evoq
%% as its snapshot_store_adapter unless these tests pass.
%%--------------------------------------------------------------------

evoq_snapshot_save_load_roundtrip(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    %% Drive through the high-level evoq snapshot API — same code path
    %% an aggregate's snapshotting hook would use.
    ok = evoq_snapshot_store:save(
        StoreId, <<"agg-snap">>, 7,
        #{counter => 42, state => running},
        #{trace_id => <<"t-1">>}),

    {ok, Loaded} = evoq_snapshot_store:load(StoreId, <<"agg-snap">>),
    ?assert(is_map(Loaded)),
    ?assertEqual(<<"agg-snap">>, maps:get(stream_id, Loaded)),
    ?assertEqual(7, maps:get(version, Loaded)),
    ?assertEqual(#{counter => 42, state => running}, maps:get(data, Loaded)),
    ?assertEqual(#{trace_id => <<"t-1">>}, maps:get(metadata, Loaded)),
    ok.

%% evoq's snapshot_store:load/2 promises a flat map. The adapter
%% returning the wrong record type would crash snapshot_to_map/1
%% with function_clause — that's the bug this test guards.
evoq_snapshot_load_returns_flat_map(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    ok = evoq_snapshot_store:save(
        StoreId, <<"agg-flatmap">>, 0, #{}, #{}),
    {ok, M} = evoq_snapshot_store:load(StoreId, <<"agg-flatmap">>),

    ?assert(is_map(M)),
    %% Envelope keys present.
    [?assert(maps:is_key(K, M))
     || K <- [stream_id, version, data, metadata, timestamp]],
    ok.

evoq_snapshot_not_found_propagates(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = mem_evoq:start_store(StoreId),

    ?assertMatch({error, not_found},
        evoq_snapshot_store:load(StoreId, <<"never-snapped">>)),
    ?assertMatch({error, not_found},
        evoq_snapshot_store:load(StoreId, <<"never-snapped">>, 7)),
    ok.
