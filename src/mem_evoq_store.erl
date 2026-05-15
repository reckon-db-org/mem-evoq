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
%% Read path (next moves — pending)
%%--------------------------------------------------------------------

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

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
    {ok, FinalVersion, State#state{streams = NewStreams}}.

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
