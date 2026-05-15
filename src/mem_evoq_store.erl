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
    case init_integrity(maps:get(integrity, Opts, disabled)) of
        {ok, Integrity} ->
            ok = mem_evoq_registry:register(StoreId, self()),
            {ok, #state{store_id = StoreId, integrity = Integrity}};
        {error, _} = Err ->
            {stop, Err}
    end.

%%--------------------------------------------------------------------
%% Write path
%%--------------------------------------------------------------------

handle_call({append, StreamId, ExpectedVersion, Events}, _From, State) ->
    case do_append(StreamId, ExpectedVersion, Events, State) of
        {ok, NewVersion, NewState} ->
            {reply, {ok, NewVersion}, NewState};
        {error, _} = Err ->
            {reply, Err, State}
    end;

%%--------------------------------------------------------------------
%% Read path
%%--------------------------------------------------------------------

handle_call({read, StreamId, FromVersion, Count, Direction}, _From, State) ->
    {reply, do_read(StreamId, FromVersion, Count, Direction, State), State};

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
%% Snapshots (further moves — pending)
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
    case check_expected_version(ExpectedVersion, CurrentVersion) of
        ok ->
            append_events_to_stream(StreamId, CurrentVersion, Events, State);
        {error, _} = Err ->
            Err
    end.

-spec current_version(binary(), #state{}) -> integer().
current_version(StreamId, #state{streams = Streams}) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            ?NO_STREAM;
        [] ->
            ?NO_STREAM;
        List when is_list(List) ->
            length(List) - 1
    end.

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
    {Recorded, FinalVersion} = lists:foldl(
        fun(Event, {Acc, AccVer}) ->
            NewVer = AccVer + 1,
            Record = create_event_record(Event, StreamId, NewVer, Now, EpochUs),
            {[Record | Acc], NewVer}
        end,
        {[], CurrentVersion},
        Events
    ),
    %% Acc is reverse-order; reverse and prepend in correct order.
    AppendList = lists:reverse(Recorded),
    NewStreams = maps_update_append(StreamId, AppendList, State#state.streams),
    NewState = State#state{streams = NewStreams},
    %% Fan out to live subscribers (move 10) — synchronous send per
    %% subscriber. Filter matching applied per event.
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
    case maps:get(Key, Map, undefined) of
        undefined ->
            maps:put(Key, ListToAppend, Map);
        Existing when is_list(Existing) ->
            maps:put(Key, Existing ++ ListToAppend, Map)
    end.

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
    binary(), non_neg_integer(), pos_integer(), forward | backward, #state{}
) -> {ok, [event()]} | {error, term()}.
do_read(StreamId, FromVersion, Count, Direction, #state{streams = Streams})
        when is_integer(FromVersion), FromVersion >= 0,
             is_integer(Count), Count > 0,
             (Direction =:= forward orelse Direction =:= backward) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            {error, {stream_not_found, StreamId}};
        Events when is_list(Events) ->
            {ok, slice_events(Events, FromVersion, Count, Direction)}
    end;
do_read(_StreamId, _FromVersion, _Count, _Direction, _State) ->
    {error, badarg}.

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
    %% Catch-up replay BEFORE installing the subscription so live
    %% deliveries that arrive after the call returns don't race with
    %% catch-up batches.
    case From of
        latest -> ok;
        N when is_integer(N), N >= 0 -> deliver_catchup(Pid, Filter, N, State);
        _ -> ok
    end,
    SubInfo = #{
        sub_key => SubKey,
        pid     => Pid,
        filter  => Filter,
        from    => From
    },
    %% Monitor the subscriber so we can self-clean on its death.
    _Ref = erlang:monitor(process, Pid),
    NewSubs = maps:put(SubKey, SubInfo, State#state.subscribers),
    {reply, {ok, SubKey}, State#state{subscribers = NewSubs}}.

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
%% The current implementation delivers ALL matching events (sorted by
%% epoch_us, offset-bounded). Future tightening could split into
%% configurable batch sizes; mem-evoq is for tests so a single batch
%% is fine.
deliver_catchup(Pid, Filter, FromOffset, #state{streams = Streams}) ->
    AllEvents = lists:append(maps:values(Streams)),
    Matched = [E || E <- AllEvents, event_matches_filter(E, Filter)],
    Sorted = lists:sort(
        fun(#event{epoch_us = A}, #event{epoch_us = B}) -> A =< B end,
        Matched),
    Sliced = case Sorted of
        _ when FromOffset =:= 0 -> Sorted;
        _ -> lists:nthtail(min(FromOffset, length(Sorted)), Sorted)
    end,
    case Sliced of
        [] -> ok;
        _ -> Pid ! {events, Sliced}, ok
    end.

%% @private Live fan-out — for each subscriber whose filter matches
%% one or more of the newly-appended events, send a batch of just
%% those matching events.
fanout_to_subscribers(NewEvents, #state{subscribers = Subs}) ->
    maps:fold(
        fun(_K, #{pid := Pid, filter := Filter}, _Acc) ->
            Matched = [E || E <- NewEvents, event_matches_filter(E, Filter)],
            case Matched of
                [] -> ok;
                _ -> Pid ! {events, Matched}, ok
            end
        end,
        ok,
        Subs).

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
