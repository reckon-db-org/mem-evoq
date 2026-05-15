%% @doc Adapter module implementing the `evoq_event_store' contract.
%%
%% Configured into evoq via:
%%
%% ```
%% application:set_env(evoq, event_store_adapter, mem_evoq_adapter).
%% '''
%%
%% Every callback looks up the store pid in {@link mem_evoq_registry}
%% and forwards via `gen_server:call/2'. The store itself
%% ({@link mem_evoq_store}) holds the actual state.
%%
%% This module is the public stable surface. The store gen_server's
%% callback contract may evolve internally; the adapter's exports must
%% not change without bumping the package's major version.
%% @end
-module(mem_evoq_adapter).

-include_lib("reckon_gater/include/reckon_gater_types.hrl").

-export([
    %% Write path
    append/4,

    %% Read path
    read/5,
    read/6,
    read_all/3,
    read_all/4,
    read_all_global/3,
    read_by_event_types/3,
    read_by_tags/3,
    read_by_tags/4,

    %% Metadata
    version/2,
    exists/2,
    has_events/1,
    list_streams/1,
    delete/2,

    %% Snapshots
    save_snapshot/3,
    save_snapshot/5,
    load_snapshot/2,
    load_snapshot_at/3,
    list_snapshots/2,
    delete_snapshot/2,

    %% Subscriptions
    subscribe/4,
    subscribe_all/3,
    unsubscribe/2
]).

%%====================================================================
%% Write
%%====================================================================

append(StoreId, StreamId, ExpectedVersion, Events) ->
    call(StoreId, {append, StreamId, ExpectedVersion, Events}).

%%====================================================================
%% Read
%%====================================================================

read(StoreId, StreamId, FromVersion, Count, Direction) ->
    call(StoreId, {read, StreamId, FromVersion, Count, Direction}).

read(StoreId, StreamId, FromVersion, Count, Direction, Opts) ->
    call(StoreId, {read, StreamId, FromVersion, Count, Direction, Opts}).

read_all(StoreId, StreamId, Direction) ->
    call(StoreId, {read_all, StreamId, Direction, 1000}).

read_all(StoreId, StreamId, Direction, BatchSize) ->
    call(StoreId, {read_all, StreamId, Direction, BatchSize}).

read_all_global(StoreId, Offset, BatchSize) ->
    call(StoreId, {read_all_global, Offset, BatchSize}).

read_by_event_types(StoreId, EventTypes, BatchSize) ->
    call(StoreId, {read_by_event_types, EventTypes, BatchSize}).

read_by_tags(StoreId, Tags, BatchSize) ->
    read_by_tags(StoreId, Tags, any, BatchSize).

read_by_tags(StoreId, Tags, Match, BatchSize) ->
    call(StoreId, {read_by_tags, Tags, Match, BatchSize}).

%%====================================================================
%% Metadata
%%====================================================================

version(StoreId, StreamId) ->
    call(StoreId, {version, StreamId}).

exists(StoreId, StreamId) ->
    boolean_or_false(call(StoreId, {exists, StreamId})).

has_events(StoreId) ->
    boolean_or_false(call(StoreId, has_events)).

boolean_or_false(true) -> true;
boolean_or_false(_)    -> false.

list_streams(StoreId) ->
    call(StoreId, list_streams).

delete(StoreId, StreamId) ->
    call(StoreId, {delete, StreamId}).

%%====================================================================
%% Snapshots
%%====================================================================

save_snapshot(StoreId, StreamId, Snapshot) ->
    call(StoreId, {save_snapshot, StreamId, Snapshot}).

save_snapshot(StoreId, StreamId, Version, Data, Metadata) ->
    call(StoreId, {save_snapshot, StreamId, Version, Data, Metadata}).

load_snapshot(StoreId, StreamId) ->
    call(StoreId, {load_snapshot, StreamId}).

load_snapshot_at(StoreId, StreamId, Version) ->
    call(StoreId, {load_snapshot, StreamId, Version}).

list_snapshots(StoreId, StreamId) ->
    call(StoreId, {list_snapshots, StreamId}).

delete_snapshot(StoreId, StreamId) ->
    call(StoreId, {delete_snapshot, StreamId}).

%%====================================================================
%% Subscriptions
%%====================================================================

subscribe(StoreId, StreamId, Pid, Opts) ->
    call(StoreId, {subscribe, StreamId, Pid, Opts}).

subscribe_all(StoreId, Pid, Opts) ->
    call(StoreId, {subscribe_all, Pid, Opts}).

unsubscribe(StoreId, SubKey) ->
    call(StoreId, {unsubscribe, SubKey}).

%%====================================================================
%% Internal — store lookup + call
%%====================================================================

call(StoreId, Request) ->
    dispatch_call(mem_evoq_registry:lookup(StoreId), StoreId, Request).

dispatch_call({error, not_found}, StoreId, _Request) ->
    {error, {store_not_started, StoreId}};
dispatch_call({ok, Pid}, StoreId, Request) ->
    try gen_server:call(Pid, Request, 5000)
    catch
        exit:{noproc, _}  -> {error, {store_not_running, StoreId}};
        exit:{timeout, _} -> {error, {timeout, StoreId}}
    end.
