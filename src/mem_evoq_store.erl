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
%% @end
-module(mem_evoq_store).
-behaviour(gen_server).

-include_lib("reckon_gater/include/reckon_gater_types.hrl").

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    store_id      :: atom(),
    streams = #{} :: #{binary() => [event()]},
    snapshots = #{} :: #{binary() => #{non_neg_integer() => snapshot()}},
    subscribers = #{} :: #{binary() => map()},
    integrity = disabled :: disabled | enabled_integrity()
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

%% Move 4 onwards will fill these out — append/4, read/5, etc.
%% For the scaffold, every callback surfaces a clean not_implemented
%% so the adapter can be wired up to evoq and the failure mode is
%% loud rather than silent.
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
%% Internal
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
