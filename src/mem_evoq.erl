%% @doc Facade for the `mem_evoq' package.
%%
%% Manages lifecycle of in-memory event stores intended for tests,
%% demos, and reference use. Pairs with {@link mem_evoq_adapter} as
%% an implementation of the `evoq_event_store' adapter behaviour.
%%
%% == Quick start ==
%%
%% ```
%% application:ensure_all_started(mem_evoq).
%% application:set_env(evoq, event_store_adapter, mem_evoq_adapter).
%%
%% {ok, _} = mem_evoq:start_store(my_test_store).
%%
%% %% Now any evoq dispatch targets the in-memory store.
%% '''
%%
%% == With integrity ==
%%
%% ```
%% Key = crypto:strong_rand_bytes(32),
%% {ok, _} = mem_evoq:start_store(my_test_store, #{
%%     integrity => #{enabled => true, key => Key}
%% }).
%% '''
%%
%% Integrity-enabled in-memory stores mirror reckon-db's behaviour
%% (HMAC + chain hash on write, verify on read, snapshot anchors)
%% so downstream consumers can test their integrity-violation
%% handling without spinning up Khepri.
%% @end
-module(mem_evoq).

-export([
    start_store/1,
    start_store/2,
    stop_store/1,
    list_stores/0
]).

-type store_opts() :: #{
    integrity => disabled |
                 #{enabled := true, key := binary()}
}.

-export_type([store_opts/0]).

%% @doc Start an in-memory store with default options (no integrity).
-spec start_store(atom()) -> {ok, pid()} | {error, term()}.
start_store(StoreId) ->
    start_store(StoreId, #{}).

%% @doc Start an in-memory store with explicit options.
%%
%% On error returns one of:
%%
%% <ul>
%%   <li>`{error, {already_started, Pid}}' if the store already exists</li>
%%   <li>`{error, {integrity_key_invalid_size, N}}' if the key is not 32 bytes</li>
%%   <li>`{error, integrity_config_invalid}' if the integrity map is malformed</li>
%% </ul>
-spec start_store(atom(), store_opts()) -> {ok, pid()} | {error, term()}.
start_store(StoreId, Opts) when is_atom(StoreId), is_map(Opts) ->
    mem_evoq_sup:start_store(StoreId, Opts).

%% @doc Stop an in-memory store and remove its child spec.
%%
%% Idempotent: returns `ok' even if the store was not running.
-spec stop_store(atom()) -> ok | {error, term()}.
stop_store(StoreId) when is_atom(StoreId) ->
    mem_evoq_sup:stop_store(StoreId).

%% @doc List all running in-memory stores.
-spec list_stores() -> [{atom(), pid()}].
list_stores() ->
    mem_evoq_registry:list().
