%% @doc Top-level supervisor.
%%
%% Boots the registry (owns the ETS table mapping StoreId -> store pid)
%% and supervises per-store {@link mem_evoq_store} processes started
%% dynamically via {@link mem_evoq:start_store/1}.
%% @end
-module(mem_evoq_sup).
-behaviour(supervisor).

-export([start_link/0, start_store/2, stop_store/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Add a per-store gen_server as a dynamic child.
-spec start_store(atom(), map()) -> {ok, pid()} | {error, term()}.
start_store(StoreId, Opts) when is_atom(StoreId), is_map(Opts) ->
    ChildSpec = #{
        id => {store, StoreId},
        start => {mem_evoq_store, start_link, [StoreId, Opts]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [mem_evoq_store]
    },
    supervisor:start_child(?MODULE, ChildSpec).

%% @doc Stop a per-store gen_server and remove its child spec.
-spec stop_store(atom()) -> ok | {error, term()}.
stop_store(StoreId) when is_atom(StoreId) ->
    case supervisor:terminate_child(?MODULE, {store, StoreId}) of
        ok ->
            _ = supervisor:delete_child(?MODULE, {store, StoreId}),
            ok;
        {error, not_found} ->
            ok;
        Error ->
            Error
    end.

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    RegistrySpec = #{
        id => mem_evoq_registry,
        start => {mem_evoq_registry, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mem_evoq_registry]
    },
    {ok, {SupFlags, [RegistrySpec]}}.
