%% @doc ETS-backed registry mapping store IDs to their per-store
%% gen_server pids.
%%
%% Owns a single named ETS table (`mem_evoq_registry`) so lookups
%% are sub-microsecond from the adapter callbacks. Lives as a
%% gen_server so the table survives supervisor restarts only if the
%% registry itself does NOT restart (which is the right semantic —
%% if the registry dies the whole adapter is unrecoverable).
%% @end
-module(mem_evoq_registry).
-behaviour(gen_server).

-export([start_link/0, register/2, unregister/1, lookup/1, list/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TABLE, ?MODULE).

%%====================================================================
%% Public API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(atom(), pid()) -> ok.
register(StoreId, Pid) when is_atom(StoreId), is_pid(Pid) ->
    gen_server:call(?MODULE, {register, StoreId, Pid}).

-spec unregister(atom()) -> ok.
unregister(StoreId) when is_atom(StoreId) ->
    gen_server:call(?MODULE, {unregister, StoreId}).

-spec lookup(atom()) -> {ok, pid()} | {error, not_found}.
lookup(StoreId) when is_atom(StoreId) ->
    case ets:lookup(?TABLE, StoreId) of
        [{StoreId, Pid}] -> {ok, Pid};
        [] -> {error, not_found}
    end.

-spec list() -> [{atom(), pid()}].
list() ->
    ets:tab2list(?TABLE).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% public so adapter callbacks can read directly without going
    %% through this process; only writes are serialised.
    ?TABLE = ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register, StoreId, Pid}, _From, State) ->
    true = ets:insert(?TABLE, {StoreId, Pid}),
    %% monitor the pid so the entry self-cleans on death
    _Ref = erlang:monitor(process, Pid),
    {reply, ok, State#{Pid => StoreId}};

handle_call({unregister, StoreId}, _From, State) ->
    true = ets:delete(?TABLE, StoreId),
    {reply, ok, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    case maps:take(Pid, State) of
        {StoreId, NewState} ->
            true = ets:delete(?TABLE, StoreId),
            {noreply, NewState};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% ETS table goes with the process. That is intentional — if the
    %% registry dies, every adapter callback should fail loudly rather
    %% than silently return stale data.
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.
