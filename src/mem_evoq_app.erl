%% @doc OTP application module for the in-memory evoq event-store adapter.
%% @end
-module(mem_evoq_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    mem_evoq_sup:start_link().

stop(_State) ->
    ok.
