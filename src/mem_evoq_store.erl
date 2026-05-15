%% @doc Per-store gen_server holding events, snapshots, subscribers,
%% and (optionally) integrity state for a single in-memory event store.
%%
%% The state shape mirrors what reckon-db keeps in Khepri:
%%
%% <ul>
%%   <li>`streams' — map of StreamId to ordered list of `#event{}'</li>
%%   <li>`snapshots' — map of StreamId to map of Version to `#snapshot{}'</li>
%%   <li>`subscribers' — map of subscription key to subscriber metadata</li>
%%   <li>`integrity' — `disabled' or `{enabled, Key, ChainStarts}'</li>
%% </ul>
%%
%% No persistence. Process restart loses state. That is the intended
%% semantic — mem-evoq exists for tests, demos, and as a reference
%% implementation of the evoq_event_store adapter behaviour. For
%% production use, pair evoq with reckon-evoq + reckon-db.
%%
%% Streams are stored in append order (events 0..N-1, oldest first).
%% This costs O(N) on append because lists are cons-prepended and
%% reversed once on the wire, but mem-evoq is for tests where N is
%% small. Premature optimisation here would obscure the reference
%% implementation aspect.
%% @end
-module(mem_evoq_store).
-behaviour(gen_server).

-include_lib("reckon_gater/include/reckon_gater_types.hrl").

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% NO_STREAM (-1), ANY_VERSION (-2), STREAM_EXISTS (-4),
%% CONTENT_TYPE_JSON come from reckon_gater_types.hrl above.

-record(state, {
    store_id           :: atom(),
    streams      = #{} :: #{binary() => [event()]},
    snapshots    = #{} :: #{binary() => #{non_neg_integer() => snapshot()}},
    subscribers  = #{} :: #{binary() => map()},
    integrity    = disabled :: disabled | enabled_integrity()
}).

-type enabled_integrity() :: #{
    key := binary(),
    chain_start := #{binary() => non_neg_integer()}
}.

%%====================================================================
%% Lifecycle
%%====================================================================

-spec start_link(atom(), map()) -> {ok, pid()} | {error, term()}.
start_link(StoreId, Opts) ->
    gen_server:start_link(?MODULE, {StoreId, Opts}, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({StoreId, Opts}) ->
    finish_init(init_integrity(maps:get(integrity, Opts, disabled)), StoreId).

finish_init({ok, Integrity}, StoreId) ->
    ok = mem_evoq_registry:register(StoreId, self()),
    {ok, #state{store_id = StoreId, integrity = Integrity}};
finish_init({error, _} = Err, _StoreId) ->
    {stop, Err}.

%%--------------------------------------------------------------------
%% Write path
%%--------------------------------------------------------------------

handle_call({append, StreamId, ExpectedVersion, Events}, _From, State) ->
    reply_append(do_append(StreamId, ExpectedVersion, Events, State), State);

%%--------------------------------------------------------------------
%% Read path
%%--------------------------------------------------------------------

handle_call({read, StreamId, FromVersion, Count, Direction}, _From, State) ->
    {reply, do_read(StreamId, FromVersion, Count, Direction, #{}, State), State};

handle_call({read, StreamId, FromVersion, Count, Direction, Opts}, _From, State) ->
    {reply, do_read(StreamId, FromVersion, Count, Direction, Opts, State), State};

%%--------------------------------------------------------------------
%% Stream metadata
%%--------------------------------------------------------------------

handle_call({version, StreamId}, _From, State) ->
    {reply, current_version(StreamId, State), State};

handle_call({exists, StreamId}, _From, State) ->
    {reply, has_stream(StreamId, State), State};

handle_call(has_events, _From, State) ->
    {reply, do_has_events(State), State};

handle_call(list_streams, _From, State) ->
    {reply, {ok, do_list_streams(State)}, State};

handle_call({delete, StreamId}, _From, State) ->
    {reply, ok, do_delete_stream(StreamId, State)};

%%--------------------------------------------------------------------
%% Append with subscriber fan-out — handled inside the {append, ...}
%% clause above; live delivery happens in do_append/4.
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Cross-stream reads
%%--------------------------------------------------------------------

handle_call({read_all_global, Offset, BatchSize}, _From, State) ->
    {reply, do_read_all_global(Offset, BatchSize, State), State};

%%--------------------------------------------------------------------
%% Subscriptions
%%--------------------------------------------------------------------

handle_call({subscribe, StreamId, Pid, Opts}, _From, State) ->
    do_subscribe({by_stream, StreamId}, Pid, Opts, State);

handle_call({subscribe_all, Pid, Opts}, _From, State) ->
    do_subscribe(all, Pid, Opts, State);

handle_call({subscribe, Type, Selector, Pid, Opts}, _From, State) ->
    do_subscribe({Type, Selector}, Pid, Opts, State);

handle_call({unsubscribe, SubKey}, _From, State) ->
    {reply, ok, do_unsubscribe(SubKey, State)};

%%--------------------------------------------------------------------
%% Snapshots
%%--------------------------------------------------------------------

handle_call({save_snapshot, StreamId, #snapshot{} = Snap0}, _From, State) ->
    reply_save_snapshot(
        apply_snapshot_integrity(StreamId, Snap0, State),
        StreamId, State);

handle_call({save_snapshot, StreamId, Version, Data, Metadata}, _From, State) ->
    Snap0 = #snapshot{
        stream_id = StreamId,
        version = Version,
        data = Data,
        metadata = Metadata,
        timestamp = erlang:system_time(millisecond)
    },
    reply_save_snapshot(
        apply_snapshot_integrity(StreamId, Snap0, State),
        StreamId, State);

handle_call({load_snapshot, StreamId}, _From, State) ->
    {reply, verify_snapshot_load(do_load_latest_snapshot(StreamId, State),
                                  StreamId, State), State};

handle_call({load_snapshot, StreamId, Version}, _From, State) ->
    {reply, verify_snapshot_load(do_load_snapshot_at(StreamId, Version, State),
                                  StreamId, State), State};

handle_call({list_snapshots, StreamId}, _From, State) ->
    {reply, {ok, do_list_snapshots(StreamId, State)}, State};

handle_call({list_snapshot_versions, StreamId}, _From, State) ->
    {reply, {ok, do_list_snapshot_versions(StreamId, State)}, State};

handle_call({delete_snapshot, StreamId}, _From, State) ->
    {reply, ok, do_delete_snapshots_for_stream(StreamId, State)};

handle_call({delete_snapshot, StreamId, Version}, _From, State) ->
    {reply, ok, do_delete_snapshot_at(StreamId, Version, State)};

%%--------------------------------------------------------------------
%% Other (further moves — pending)
%%--------------------------------------------------------------------

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Subscriber pid died — clean up any subscriptions it owned.
handle_info({'DOWN', _Ref, process, Pid, _Reason},
            #state{subscribers = Subs} = State) ->
    Kept = maps:filter(
        fun(_K, #{pid := P}) -> P =/= Pid end,
        Subs),
    {noreply, State#state{subscribers = Kept}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{store_id = StoreId}) ->
    catch mem_evoq_registry:unregister(StoreId),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal — append
%%====================================================================

-spec do_append(binary(), integer(), [map()], #state{}) ->
    {ok, non_neg_integer(), #state{}} | {error, term()}.
do_append(StreamId, ExpectedVersion, Events, State) ->
    CurrentVersion = current_version(StreamId, State),
    proceed_with_append(
        check_expected_version(ExpectedVersion, CurrentVersion),
        StreamId, CurrentVersion, Events, State).

proceed_with_append(ok, StreamId, CurrentVersion, Events, State) ->
    append_events_to_stream(StreamId, CurrentVersion, Events, State);
proceed_with_append({error, _} = Err, _StreamId, _CV, _Events, _State) ->
    Err.

reply_append({ok, NewVersion, NewState}, _OldState) ->
    {reply, {ok, NewVersion}, NewState};
reply_append({error, _} = Err, State) ->
    {reply, Err, State}.

-spec current_version(binary(), #state{}) -> integer().
current_version(StreamId, #state{streams = Streams}) ->
    version_from_entry(maps:get(StreamId, Streams, undefined)).

version_from_entry(undefined)                    -> ?NO_STREAM;
version_from_entry([])                           -> ?NO_STREAM;
version_from_entry(List) when is_list(List)      -> length(List) - 1.

%% Mirror of reckon_db_streams:check_expected_version/2.
-spec check_expected_version(integer(), integer()) -> ok | {error, term()}.
check_expected_version(?ANY_VERSION, _CurrentVersion) ->
    ok;
check_expected_version(?NO_STREAM, ?NO_STREAM) ->
    ok;
check_expected_version(?NO_STREAM, CurrentVersion) ->
    {error, {wrong_expected_version, ?NO_STREAM, CurrentVersion}};
check_expected_version(?STREAM_EXISTS, ?NO_STREAM) ->
    {error, {wrong_expected_version, ?STREAM_EXISTS, ?NO_STREAM}};
check_expected_version(?STREAM_EXISTS, _CurrentVersion) ->
    ok;
check_expected_version(Expected, Current) when Expected =:= Current ->
    ok;
check_expected_version(Expected, Current) ->
    {error, {wrong_expected_version, Expected, Current}}.

-spec append_events_to_stream(
    binary(), integer(), [map()], #state{}
) -> {ok, non_neg_integer(), #state{}}.
append_events_to_stream(StreamId, CurrentVersion, Events, State) ->
    Now = erlang:system_time(millisecond),
    EpochUs = erlang:system_time(microsecond),
    IntegrityCtx = setup_integrity_for_append(StreamId, CurrentVersion, State),
    InitialTip = resolve_write_initial_tip(
        StreamId, CurrentVersion + 1, IntegrityCtx, State),
    {Recorded, FinalVersion, _FinalTip} = lists:foldl(
        fun(Event, {Acc, AccVer, AccTip}) ->
            NewVer = AccVer + 1,
            Record0 = create_event_record(Event, StreamId, NewVer, Now, EpochUs),
            {Record, NextTip} = apply_integrity_if_enabled(
                Record0, AccTip, IntegrityCtx),
            {[Record | Acc], NewVer, NextTip}
        end,
        {[], CurrentVersion, InitialTip},
        Events
    ),
    %% Acc is reverse-order; reverse and prepend in correct order.
    AppendList = lists:reverse(Recorded),
    NewStreams = maps_update_append(StreamId, AppendList, State#state.streams),
    %% Persist the watermark side-effect from setup_integrity_for_append
    %% (only matters on integrity-enabled stores, no-op otherwise).
    NewIntegrity = persist_watermark(StreamId, IntegrityCtx, State#state.integrity),
    NewState = State#state{streams = NewStreams, integrity = NewIntegrity},
    %% Fan out to live subscribers — synchronous send per subscriber.
    ok = fanout_to_subscribers(AppendList, NewState),
    {ok, FinalVersion, NewState}.

%% Mirror of reckon_db_streams:create_event_record/5. Integrity fields
%% (prev_event_hash, mac, signature) are left as undefined here —
%% those will be populated when move 14 wires integrity in.
-spec create_event_record(
    map(), binary(), non_neg_integer(), integer(), integer()
) -> event().
create_event_record(Event, StreamId, Version, Timestamp, EpochUs) ->
    EventId             = maps:get(event_id, Event, generate_event_id()),
    EventType           = maps:get(event_type, Event),
    Data                = maps:get(data, Event),
    Metadata            = maps:get(metadata, Event, #{}),
    Tags                = maps:get(tags, Event, undefined),
    DataContentType     = maps:get(data_content_type, Event, ?CONTENT_TYPE_JSON),
    MetadataContentType = maps:get(metadata_content_type, Event, ?CONTENT_TYPE_JSON),
    #event{
        event_id              = EventId,
        event_type            = EventType,
        stream_id             = StreamId,
        version               = Version,
        data                  = Data,
        metadata              = Metadata,
        tags                  = Tags,
        timestamp             = Timestamp,
        epoch_us              = EpochUs,
        data_content_type     = DataContentType,
        metadata_content_type = MetadataContentType
    }.

%% Append-to-list in a map; create the entry if absent. Order matters:
%% List MUST be in version order (oldest first).
%%
%% Empty appends are a no-op — never create an empty stream entry.
%% This matches reckon-db, which doesn't put anything in Khepri if
%% no events are provided.
maps_update_append(_Key, [], Map) ->
    Map;
maps_update_append(Key, ListToAppend, Map) ->
    insert_or_extend(maps:get(Key, Map, undefined), Key, ListToAppend, Map).

insert_or_extend(undefined, Key, ListToAppend, Map) ->
    maps:put(Key, ListToAppend, Map);
insert_or_extend(Existing, Key, ListToAppend, Map) when is_list(Existing) ->
    maps:put(Key, Existing ++ ListToAppend, Map).

%% Generate a UUIDv7-shaped event id. mem-evoq is for tests, so a
%% per-process counter + timestamp is fine — strict v7 collision-
%% resistance is not the property tests care about.
generate_event_id() ->
    Bin = crypto:strong_rand_bytes(16),
    list_to_binary(
        [hex_digit(B) || <<B:4>> <= Bin]
    ).

hex_digit(D) when D < 10 -> $0 + D;
hex_digit(D)             -> $a + D - 10.

%%====================================================================
%% Internal — read
%%====================================================================

%% @private Read a slice of a stream.
%%
%% Forward semantics: take events at versions
%% `[FromVersion, FromVersion + Count - 1]'.
%% Backward semantics: take events at versions
%% `[max(0, FromVersion - Count + 1), FromVersion]', returned in
%% descending version order (newest first).
%%
%% Stream-not-found returns `{error, {stream_not_found, StreamId}}'.
%% Out-of-range requests return a partial result (no padding, no error).
-spec do_read(
    binary(), non_neg_integer(), pos_integer(), forward | backward,
    map(), #state{}
) -> {ok, [event()]} | {error, term()}.
do_read(StreamId, FromVersion, Count, Direction, Opts,
        #state{streams = Streams} = State)
        when is_integer(FromVersion), FromVersion >= 0,
             is_integer(Count), Count > 0,
             (Direction =:= forward orelse Direction =:= backward),
             is_map(Opts) ->
    read_with_lookup(
        maps:get(StreamId, Streams, undefined),
        StreamId, FromVersion, Count, Direction, Opts, State);
do_read(_StreamId, _FromVersion, _Count, _Direction, _Opts, _State) ->
    {error, badarg}.

read_with_lookup(undefined, StreamId, _F, _C, _D, _O, _S) ->
    {error, {stream_not_found, StreamId}};
read_with_lookup(Events, StreamId, FromVersion, Count, Direction, Opts, State)
        when is_list(Events) ->
    maybe_verify_read(
        StreamId, FromVersion, Direction,
        slice_events(Events, FromVersion, Count, Direction),
        Opts, State).

%%--------------------------------------------------------------------
%% Read-path verification (move 15)
%%--------------------------------------------------------------------

maybe_verify_read(StreamId, FromVersion, Direction, Events, Opts, State) ->
    Mode = maps:get(verify, Opts, skip_legacy),
    dispatch_verify_read(
        verify_required(State#state.integrity, Mode),
        StreamId, FromVersion, Direction, Events, Mode, State).

dispatch_verify_read(false, _StreamId, _FromVersion, _Direction, Events, _Mode, _State) ->
    {ok, Events};
dispatch_verify_read(true, StreamId, FromVersion, Direction, Events, Mode, State) ->
    finalize_verify_read(
        verify_in_direction(StreamId, FromVersion, Direction, Events, Mode, State)).

finalize_verify_read({ok, _} = Ok)                       -> Ok;
finalize_verify_read({integrity_violation, _} = V)       -> {error, V}.

verify_required(disabled, _Mode) -> false;
verify_required(_, skip_all)     -> false;
verify_required(_, _)            -> true.

verify_in_direction(StreamId, FromVersion, forward, Events, Mode, State) ->
    verify_events_forward(StreamId, FromVersion, Events, Mode, State);
verify_in_direction(StreamId, FromVersion, backward, Events, Mode, State) ->
    %% Same trick as reckon-db 2.1.1: reverse to forward, verify, reverse back.
    Forward = lists:reverse(Events),
    reverse_verified(
        verify_events_forward(
            StreamId, forward_start_version(Forward, FromVersion),
            Forward, Mode, State)).

forward_start_version([], FallbackVersion)              -> FallbackVersion;
forward_start_version([#event{version = V} | _], _)     -> V.

reverse_verified({ok, Events}) -> {ok, lists:reverse(Events)};
reverse_verified(Other)        -> Other.

verify_events_forward(StreamId, StartVersion, Events, Mode,
                      #state{integrity = #{key := Key, chain_start := WMs},
                             streams = Streams}) ->
    ChainStart = maps:get(StreamId, WMs, undefined),
    InitialTip = resolve_read_initial_tip(
        StreamId, StartVersion, ChainStart, Streams),
    verify_events_loop(Events, InitialTip, ChainStart, Key, Mode, StreamId, []).

%% Mirrors reckon_db_streams:resolve_read_initial_tip/4 — see that
%% module for the rationale on each branch.
resolve_read_initial_tip(_StreamId, _StartVersion, undefined, _Streams) ->
    undefined;
resolve_read_initial_tip(_StreamId, StartVersion, ChainStart, _Streams)
        when StartVersion < ChainStart ->
    undefined;
resolve_read_initial_tip(_StreamId, StartVersion, ChainStart, _Streams)
        when StartVersion =:= ChainStart ->
    reckon_gater_integrity:genesis_prev_hash();
resolve_read_initial_tip(StreamId, StartVersion, _ChainStart, Streams)
        when StartVersion > 0 ->
    %% Predecessor must be on disk and integrity-bearing; look it up
    %% in the live in-memory streams map.
    tip_of_predecessor(
        find_event_at(maps:get(StreamId, Streams, []), StartVersion - 1)).

tip_of_predecessor({ok, #event{prev_event_hash = Prev} = E})
        when is_binary(Prev) ->
    reckon_gater_integrity:compute_chain_hash(E, Prev);
tip_of_predecessor(_) ->
    undefined.

verify_events_loop([], _Tip, _ChainStart, _Key, _Mode, _StreamId, Acc) ->
    {ok, lists:reverse(Acc)};
verify_events_loop([Event | Rest], Tip, ChainStart, Key, Mode, StreamId, Acc) ->
    branch_on_event_kind(
        is_legacy_event(Event, ChainStart),
        Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc).

branch_on_event_kind(true, Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc) ->
    handle_legacy(Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc);
branch_on_event_kind(false, Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc) ->
    handle_integrity(Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc).

is_legacy_event(_Event, undefined) -> true;
is_legacy_event(#event{version = V}, ChainStart) -> V < ChainStart.

handle_legacy(Event, _Rest, _Tip, _ChainStart, _Key, strict, StreamId, _Acc) ->
    {integrity_violation, #{
        layer => storage,
        stream_id => StreamId,
        version => Event#event.version,
        kind => missing_integrity,
        context => #{detail => legacy_event_under_strict_mode}
    }};
handle_legacy(Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc) ->
    verify_events_loop(Rest, Tip, ChainStart, Key, Mode, StreamId, [Event | Acc]).

handle_integrity(Event, Rest, undefined, ChainStart, Key, Mode, StreamId, Acc)
        when is_integer(ChainStart),
             Event#event.version =:= ChainStart ->
    %% First integrity event in the stream — seed tip with genesis.
    handle_integrity(Event, Rest,
                     reckon_gater_integrity:genesis_prev_hash(),
                     ChainStart, Key, Mode, StreamId, Acc);
handle_integrity(Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc)
        when is_binary(Tip) ->
    continue_after_verify(
        reckon_gater_integrity:verify_event(Event, Tip, Key),
        Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc).

continue_after_verify(ok, Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc) ->
    NextTip = reckon_gater_integrity:compute_chain_hash(Event, Tip),
    verify_events_loop(Rest, NextTip, ChainStart, Key, Mode, StreamId, [Event | Acc]);
continue_after_verify({integrity_violation, _} = V, _E, _R, _T, _C, _K, _M, _S, _A) ->
    V.

%% List is in forward order (oldest first); filter into the requested
%% window, then reverse for backward reads so callers get the natural
%% "newest first" sequence.
slice_events(Events, FromVersion, Count, forward) ->
    EndVersion = FromVersion + Count - 1,
    [E || E <- Events,
          E#event.version >= FromVersion,
          E#event.version =< EndVersion];
slice_events(Events, FromVersion, Count, backward) ->
    StartVersion = max(0, FromVersion - Count + 1),
    Filtered = [E || E <- Events,
                     E#event.version >= StartVersion,
                     E#event.version =< FromVersion],
    lists:reverse(Filtered).

%%====================================================================
%% Internal — metadata
%%====================================================================

%% @private Stream exists when it has at least one event in state.
%% Empty entries are not produced (see maps_update_append/3).
-spec has_stream(binary(), #state{}) -> boolean().
has_stream(StreamId, #state{streams = Streams}) ->
    maps:is_key(StreamId, Streams).

%% @private Whether the store contains at least one event across any
%% stream. Mirrors reckon_db_streams:has_events/1.
-spec do_has_events(#state{}) -> boolean().
do_has_events(#state{streams = Streams}) ->
    %% Empty-stream entries don't exist by construction, so any
    %% non-empty map means events exist.
    maps:size(Streams) > 0.

%% @private All stream IDs in the store. Order is implementation-
%% defined; mirrors reckon_db_streams:list_streams/1 which returns a
%% lists:usort'd list.
-spec do_list_streams(#state{}) -> [binary()].
do_list_streams(#state{streams = Streams}) ->
    lists:usort(maps:keys(Streams)).

%% @private Delete a stream from the store. Idempotent — returns
%% an unchanged state if the stream doesn't exist. Snapshots for
%% the stream are also dropped (matches the operational expectation
%% that "delete the stream" means "remove all trace of it").
-spec do_delete_stream(binary(), #state{}) -> #state{}.
do_delete_stream(StreamId, #state{streams = S, snapshots = SS} = State) ->
    State#state{
        streams   = maps:remove(StreamId, S),
        snapshots = maps:remove(StreamId, SS)
    }.

%%====================================================================
%% Internal — cross-stream reads
%%====================================================================

%% @private Read all events across all streams, sorted by epoch_us,
%% skipping `Offset` events and returning up to `BatchSize` events.
%%
%% This is the catch-up subscription primitive; reckon-db's
%% reckon_db_streams:read_all_global/3 does the same thing against
%% Khepri. Within a single store instance ties on epoch_us are
%% rare but possible (microsecond timestamps); we don't impose a
%% secondary sort because reckon-db doesn't either.
-spec do_read_all_global(non_neg_integer(), pos_integer(), #state{}) ->
    {ok, [event()]} | {error, term()}.
do_read_all_global(Offset, BatchSize, #state{streams = Streams})
        when is_integer(Offset), Offset >= 0,
             is_integer(BatchSize), BatchSize > 0 ->
    AllEvents = lists:append(maps:values(Streams)),
    Sorted = lists:sort(
        fun(#event{epoch_us = A}, #event{epoch_us = B}) -> A =< B end,
        AllEvents
    ),
    Sliced = lists:sublist(Sorted, Offset + 1, BatchSize),
    {ok, Sliced};
do_read_all_global(_Offset, _BatchSize, _State) ->
    {error, badarg}.

%%====================================================================
%% Internal — subscriptions
%%====================================================================

-type filter() ::
    all |
    {by_stream, binary()} |
    {by_event_type, binary()} |
    {by_event_pattern, map()} |
    {by_tags, [binary()], any | all}.

%% @private Register a subscription.
%%
%% Opts:
%% <ul>
%%   <li>`from => 0' — replay all matching existing events synchronously
%%       to the subscriber pid before the call returns (catch-up).</li>
%%   <li>`from => latest' (default) — no catch-up; subscriber receives
%%       only events written after the call returns (live).</li>
%% </ul>
%%
%% Returns `{ok, SubKey}' where SubKey is an opaque binary the caller
%% later passes to `unsubscribe/2'.
do_subscribe(FilterIn, Pid, Opts, State) when is_pid(Pid), is_map(Opts) ->
    SubKey = generate_sub_key(),
    From = maps:get(from, Opts, latest),
    Filter = normalise_filter(FilterIn),
    %% Catch-up BEFORE installing the subscription so live deliveries
    %% arriving after this call returns don't race catch-up batches.
    ok = maybe_deliver_catchup(From, Pid, Filter, State),
    SubInfo = #{sub_key => SubKey, pid => Pid,
                filter => Filter, from => From},
    _Ref = erlang:monitor(process, Pid),
    NewSubs = maps:put(SubKey, SubInfo, State#state.subscribers),
    {reply, {ok, SubKey}, State#state{subscribers = NewSubs}}.

maybe_deliver_catchup(latest, _Pid, _Filter, _State) ->
    ok;
maybe_deliver_catchup(N, Pid, Filter, State) when is_integer(N), N >= 0 ->
    deliver_catchup(Pid, Filter, N, State);
maybe_deliver_catchup(_, _Pid, _Filter, _State) ->
    ok.

%% @private Remove a subscription. Idempotent.
do_unsubscribe(SubKey, #state{subscribers = Subs} = State) ->
    State#state{subscribers = maps:remove(SubKey, Subs)}.

%% @private Subscribe-time normalisation of the user-facing filter
%% shapes (stream / event_type / event_pattern / tags — both the
%% bare-atom and the `by_*` flavours) to the internal canonical form.
normalise_filter(all) -> all;
normalise_filter({stream, StreamId}) -> {by_stream, StreamId};
normalise_filter({by_stream, StreamId}) -> {by_stream, StreamId};
normalise_filter({event_type, Type}) -> {by_event_type, Type};
normalise_filter({by_event_type, Type}) -> {by_event_type, Type};
normalise_filter({event_pattern, P}) -> {by_event_pattern, P};
normalise_filter({by_event_pattern, P}) -> {by_event_pattern, P};
normalise_filter({tags, Tags}) -> {by_tags, Tags, any};
normalise_filter({by_tags, Tags}) -> {by_tags, Tags, any};
normalise_filter({by_tags, Tags, Match}) -> {by_tags, Tags, Match}.

%% @private Send a catch-up batch synchronously.
%%
%% For integrity-enabled stores, each event's MAC is verified before
%% delivery. A failure halts the catch-up — the subscriber receives a
%% {subscription_error, {integrity_violation, _}} message and no
%% further events. Same shape as reckon-db's subscription catch-up
%% (move 17).
deliver_catchup(Pid, Filter, FromOffset, #state{streams = Streams} = State) ->
    AllEvents = lists:append(maps:values(Streams)),
    Matched = [E || E <- AllEvents, event_matches_filter(E, Filter)],
    Sorted = lists:sort(
        fun(#event{epoch_us = A}, #event{epoch_us = B}) -> A =< B end,
        Matched),
    Sliced = slice_catchup(Sorted, FromOffset),
    dispatch_catchup(verify_catchup_batch(Sliced, State), Pid, Sliced).

slice_catchup(Sorted, 0) ->
    Sorted;
slice_catchup(Sorted, FromOffset) ->
    lists:nthtail(min(FromOffset, length(Sorted)), Sorted).

dispatch_catchup(ok, _Pid, []) ->
    ok;
dispatch_catchup(ok, Pid, Sliced) ->
    Pid ! {events, Sliced},
    ok;
dispatch_catchup({integrity_violation, _} = V, Pid, _Sliced) ->
    Pid ! {subscription_error, V},
    ok.

%% @private MAC-only verification across an out-of-order set of
%% events (catch-up may include multiple streams). Per-stream chain
%% verification is a different concern handled in the read path;
%% here we just confirm each event's MAC against the store's key.
%% Disabled stores skip entirely.
verify_catchup_batch(_Events, #state{integrity = disabled}) ->
    ok;
verify_catchup_batch(Events, #state{integrity = #{key := Key}}) ->
    verify_each_event_mac(Events, Key).

verify_each_event_mac([], _Key) ->
    ok;
verify_each_event_mac([Event | Rest], Key) ->
    continue_mac_chain(verify_one_event_mac(Event, Key), Rest, Key).

continue_mac_chain(ok, Rest, Key)               -> verify_each_event_mac(Rest, Key);
continue_mac_chain(Violation, _Rest, _Key)      -> Violation.

verify_one_event_mac(#event{mac = undefined}, _Key) ->
    %% Legacy event in catch-up — pass through (matches skip_legacy
    %% semantics from the read path).
    ok;
verify_one_event_mac(#event{mac = {_KeyId, StoredMac}} = Event, Key) ->
    Stripped = Event#event{mac = undefined, signature = undefined},
    Bytes = reckon_gater_canonical:encode_for_mac(event, Stripped),
    Expected = crypto:mac(hmac, sha256, Key, Bytes),
    mac_match_outcome(crypto:hash_equals(StoredMac, Expected), Event).

mac_match_outcome(true, _Event) ->
    ok;
mac_match_outcome(false, Event) ->
    {integrity_violation, #{
        layer => storage,
        stream_id => Event#event.stream_id,
        version => Event#event.version,
        kind => mac_mismatch,
        context => #{detected_at => catchup_replay}
    }}.

%% @private Live fan-out — for each subscriber whose filter matches
%% one or more of the newly-appended events, send a batch of just
%% those matching events.
fanout_to_subscribers(NewEvents, #state{subscribers = Subs}) ->
    maps:fold(
        fun(_K, #{pid := Pid, filter := Filter}, _Acc) ->
            deliver_matched(
                Pid, [E || E <- NewEvents, event_matches_filter(E, Filter)])
        end,
        ok, Subs).

deliver_matched(_Pid, [])      -> ok;
deliver_matched(Pid, Matched)  -> Pid ! {events, Matched}, ok.

%% @private Single-event filter match. Move 11 — the filter taxonomy
%% covered here is a subset of reckon-db's (which adds payload-pattern
%% and by_event_pattern variants). For mem-evoq we cover the common
%% test cases and document the rest.
-spec event_matches_filter(event(), filter()) -> boolean().
event_matches_filter(_Event, all) ->
    true;
event_matches_filter(#event{stream_id = SID}, {by_stream, SID}) ->
    true;
event_matches_filter(#event{}, {by_stream, _}) ->
    false;
event_matches_filter(#event{event_type = T}, {by_event_type, T}) ->
    true;
event_matches_filter(#event{}, {by_event_type, _}) ->
    false;
event_matches_filter(#event{event_type = T}, {by_event_pattern, #{event_type := T}}) ->
    true;
event_matches_filter(#event{}, {by_event_pattern, _}) ->
    false;
event_matches_filter(#event{tags = Tags}, {by_tags, Wanted, Match})
        when is_list(Tags) ->
    tags_match(Tags, Wanted, Match);
event_matches_filter(#event{tags = undefined}, {by_tags, _, _}) ->
    false.

tags_match(EventTags, Wanted, any) ->
    lists:any(fun(T) -> lists:member(T, EventTags) end, Wanted);
tags_match(EventTags, Wanted, all) ->
    lists:all(fun(T) -> lists:member(T, EventTags) end, Wanted).

generate_sub_key() ->
    Bin = crypto:strong_rand_bytes(8),
    list_to_binary([hex_digit(B) || <<B:4>> <= Bin]).

%%====================================================================
%% Internal — snapshots
%%====================================================================

%% @private Save a snapshot record under StreamId/Version. Replaces
%% any existing snapshot at the same version.
do_save_snapshot(StreamId, #snapshot{version = V} = Snap,
                 #state{snapshots = All} = State) ->
    PerStream = maps:get(StreamId, All, #{}),
    NewPerStream = maps:put(V, Snap, PerStream),
    State#state{snapshots = maps:put(StreamId, NewPerStream, All)}.

%% @private Load the highest-version snapshot for a stream, or
%% `{error, not_found}'.
do_load_latest_snapshot(StreamId, #state{snapshots = All}) ->
    latest_in_per_stream(maps:get(StreamId, All, undefined)).

latest_in_per_stream(undefined) ->
    {error, not_found};
latest_in_per_stream(PerStream) when map_size(PerStream) =:= 0 ->
    {error, not_found};
latest_in_per_stream(PerStream) ->
    Latest = lists:max(maps:keys(PerStream)),
    {ok, maps:get(Latest, PerStream)}.

%% @private Load a specific version snapshot.
do_load_snapshot_at(StreamId, Version, #state{snapshots = All}) ->
    snapshot_at_version(maps:get(StreamId, All, undefined), Version).

snapshot_at_version(undefined, _Version) ->
    {error, not_found};
snapshot_at_version(PerStream, Version) ->
    wrap_snapshot(maps:get(Version, PerStream, undefined)).

wrap_snapshot(undefined) -> {error, not_found};
wrap_snapshot(Snap)      -> {ok, Snap}.

%% @private List all snapshots for a stream, oldest version first.
do_list_snapshots(StreamId, #state{snapshots = All}) ->
    snapshots_sorted(maps:get(StreamId, All, undefined)).

snapshots_sorted(undefined) ->
    [];
snapshots_sorted(PerStream) ->
    Versions = lists:sort(maps:keys(PerStream)),
    [maps:get(V, PerStream) || V <- Versions].

%% @private Versions only (ascending). Used by
%% evoq_snapshot_adapter:list_versions/2.
do_list_snapshot_versions(StreamId, #state{snapshots = All}) ->
    versions_sorted(maps:get(StreamId, All, undefined)).

versions_sorted(undefined) ->
    [];
versions_sorted(PerStream) ->
    lists:sort(maps:keys(PerStream)).

%% @private Delete all snapshots for a stream. Idempotent.
do_delete_snapshots_for_stream(StreamId, #state{snapshots = All} = State) ->
    State#state{snapshots = maps:remove(StreamId, All)}.

%% @private Delete a single version's snapshot. Idempotent —
%% removes nothing if the stream / version are absent.
do_delete_snapshot_at(StreamId, Version, #state{snapshots = All} = State) ->
    State#state{snapshots = drop_version(
        maps:get(StreamId, All, undefined), StreamId, Version, All)}.

drop_version(undefined, _StreamId, _Version, All) ->
    All;
drop_version(PerStream, StreamId, Version, All) ->
    reinsert_or_remove(maps:remove(Version, PerStream), StreamId, All).

reinsert_or_remove(Empty, StreamId, All) when map_size(Empty) =:= 0 ->
    maps:remove(StreamId, All);
reinsert_or_remove(PerStream, StreamId, All) ->
    maps:put(StreamId, PerStream, All).

%%====================================================================
%% Internal — snapshot integrity (move 16)
%%====================================================================

%% @private Apply snapshot integrity if enabled on the store. For
%% integrity-disabled stores this is a pass-through. For
%% integrity-enabled stores: compute the anchor_hash from the
%% underlying event at the snapshot version, compute the snapshot
%% MAC, populate both fields. Refuses the save if no integrity-
%% bearing event exists at the snapshot version.
apply_snapshot_integrity(_StreamId, Snap, #state{integrity = disabled}) ->
    {ok, Snap};
apply_snapshot_integrity(StreamId, #snapshot{version = V} = Snap,
                         #state{integrity = #{key := Key}, streams = Streams}) ->
    seal_snapshot(compute_event_chain_hash(StreamId, V, Streams), Snap, Key).

seal_snapshot({ok, AnchorHash}, Snap, Key) ->
    Snap1 = Snap#snapshot{anchor_hash = AnchorHash},
    Mac = reckon_gater_integrity:compute_snapshot_mac(Snap1, Key),
    {ok, Snap1#snapshot{mac = Mac}};
seal_snapshot({error, _} = Err, _Snap, _Key) ->
    Err.

reply_save_snapshot({ok, Snap}, StreamId, State) ->
    {reply, ok, do_save_snapshot(StreamId, Snap, State)};
reply_save_snapshot({error, _} = Err, _StreamId, State) ->
    {reply, Err, State}.

%% @private Compute the chain hash of the event at (StreamId, Version).
%% Used to seed the anchor when saving a snapshot and to recompute
%% the actual anchor when verifying one.
compute_event_chain_hash(StreamId, Version, Streams) ->
    chain_hash_for(
        find_event_at(maps:get(StreamId, Streams, []), Version),
        StreamId, Version).

chain_hash_for({ok, #event{prev_event_hash = Prev} = E}, _StreamId, _Version)
        when is_binary(Prev) ->
    {ok, reckon_gater_integrity:compute_chain_hash(E, Prev)};
chain_hash_for({ok, #event{prev_event_hash = undefined}}, StreamId, Version) ->
    {error, {snapshot_anchor_unavailable,
             #{stream_id => StreamId, version => Version,
               reason => event_is_legacy}}};
chain_hash_for(_, StreamId, Version) ->
    {error, {snapshot_anchor_unavailable,
             #{stream_id => StreamId, version => Version,
               reason => event_not_found}}}.

%% @private If integrity is enabled and the loaded snapshot carries
%% integrity fields, verify both the anchor and the MAC. Legacy
%% snapshots (no integrity fields) pass through. Integrity-disabled
%% stores skip verification entirely.
verify_snapshot_load({error, _} = E, _StreamId, _State) ->
    E;
verify_snapshot_load({ok, _} = Ok, _StreamId, #state{integrity = disabled}) ->
    Ok;
verify_snapshot_load({ok, Snap} = Ok, StreamId, State) ->
    branch_on_snapshot_kind(
        reckon_gater_integrity:is_legacy_snapshot(Snap),
        Snap, Ok, StreamId, State).

branch_on_snapshot_kind(true, _Snap, Ok, _StreamId, _State) ->
    Ok;
branch_on_snapshot_kind(false, Snap, Ok, StreamId,
                        #state{integrity = #{key := Key}, streams = Streams}) ->
    verify_against_recomputed_anchor(
        compute_event_chain_hash(StreamId, Snap#snapshot.version, Streams),
        Snap, Ok, Key).

verify_against_recomputed_anchor({error, _} = Err, _Snap, _Ok, _Key) ->
    Err;
verify_against_recomputed_anchor({ok, Anchor}, Snap, Ok, Key) ->
    snapshot_verification_outcome(
        reckon_gater_integrity:verify_snapshot(Snap, Anchor, Key), Ok).

snapshot_verification_outcome(ok, Ok)                            -> Ok;
snapshot_verification_outcome({integrity_violation, _} = V, _Ok) -> {error, V}.

%%====================================================================
%% Internal — integrity init
%%====================================================================

init_integrity(disabled) ->
    {ok, disabled};
init_integrity(#{enabled := true, key := Key})
        when is_binary(Key), byte_size(Key) =:= 32 ->
    {ok, #{key => Key, chain_start => #{}}};
init_integrity(#{enabled := true, key := Key}) when is_binary(Key) ->
    {error, {integrity_key_invalid_size, byte_size(Key)}};
init_integrity(_) ->
    {error, integrity_config_invalid}.

%%====================================================================
%% Internal — write-path integrity (move 14)
%%====================================================================

%% @private Determine the integrity context for a single append batch.
%% For disabled stores this is the constant atom `disabled' (the
%% per-event code paths short-circuit on it). For enabled stores we
%% resolve (or lazily set) the per-stream chain_start watermark and
%% return it along with the key so the per-event helpers don't have
%% to keep hitting state.
-type integrity_ctx() ::
    disabled |
    {enabled, Key :: binary(), ChainStart :: non_neg_integer()}.

-spec setup_integrity_for_append(binary(), integer(), #state{}) ->
    integrity_ctx().
setup_integrity_for_append(_StreamId, _CurrentVersion, #state{integrity = disabled}) ->
    disabled;
setup_integrity_for_append(StreamId, CurrentVersion,
                           #state{integrity = #{key := Key, chain_start := Map}}) ->
    NextVersion = CurrentVersion + 1,
    %% Lazy enablement: if the stream has no watermark, the upcoming
    %% append becomes the first integrity-bearing event in that stream.
    {enabled, Key,
     chain_start_or_default(maps:get(StreamId, Map, undefined), NextVersion)}.

chain_start_or_default(undefined, NextVersion) -> NextVersion;
chain_start_or_default(V, _NextVersion)        -> V.

%% @private Persist the chain_start watermark set during setup. No-op
%% for stores where integrity is disabled.
persist_watermark(_StreamId, disabled, _Integrity) ->
    disabled;
persist_watermark(StreamId, {enabled, _Key, ChainStart},
                  #{chain_start := Map} = Integrity) ->
    Integrity#{chain_start := maps:put(StreamId, ChainStart, Map)}.

%% @private Resolve the chain-tip value that the first event in this
%% batch must reference as its `prev_event_hash'.
%%
%% Disabled: undefined (unused).
%% Enabled, first integrity event in stream: genesis.
%% Enabled, later batch: chain hash of the predecessor on disk.
-spec resolve_write_initial_tip(
    binary(), non_neg_integer(), integrity_ctx(), #state{}
) -> binary() | undefined.
resolve_write_initial_tip(_StreamId, _NextVersion, disabled, _State) ->
    undefined;
resolve_write_initial_tip(_StreamId, NextVersion, {enabled, _Key, ChainStart}, _State)
        when NextVersion =:= ChainStart ->
    reckon_gater_integrity:genesis_prev_hash();
resolve_write_initial_tip(StreamId, NextVersion, {enabled, _Key, ChainStart},
                          #state{streams = Streams})
        when NextVersion > ChainStart ->
    PrevVersion = NextVersion - 1,
    write_tip_from_predecessor(
        find_event_at(maps:get(StreamId, Streams, []), PrevVersion),
        StreamId, PrevVersion, ChainStart).

write_tip_from_predecessor(
    {ok, #event{prev_event_hash = PrevPrev} = PrevEvent},
    _StreamId, _PrevVersion, _ChainStart) when is_binary(PrevPrev) ->
    reckon_gater_integrity:compute_chain_hash(PrevEvent, PrevPrev);
write_tip_from_predecessor(_, StreamId, PrevVersion, ChainStart) ->
    erlang:error({integrity_setup_failed,
                  #{stream_id => StreamId,
                    looking_for_version => PrevVersion,
                    chain_start => ChainStart}}).

find_event_at([], _Version) -> error;
find_event_at([#event{version = V} = E | _], V) -> {ok, E};
find_event_at([#event{version = V} | _], Target) when V > Target -> error;
find_event_at([_ | Rest], Target) -> find_event_at(Rest, Target).

%% @private Compute and attach integrity fields for one event. For
%% disabled stores this is a pass-through.
-spec apply_integrity_if_enabled(
    event(), binary() | undefined, integrity_ctx()
) -> {event(), binary() | undefined}.
apply_integrity_if_enabled(Event, _Tip, disabled) ->
    {Event, undefined};
apply_integrity_if_enabled(Event, Tip, {enabled, Key, _ChainStart})
        when is_binary(Tip) ->
    Event1 = Event#event{prev_event_hash = Tip},
    Mac = reckon_gater_integrity:compute_event_mac(Event1, Key),
    Event2 = Event1#event{mac = Mac},
    NextTip = reckon_gater_integrity:compute_chain_hash(Event2, Tip),
    {Event2, NextTip}.
