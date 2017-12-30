%%%-------------------------------------------------------------------
%%% @copyright (C) 2013, 2600Hz
%%% @doc
%%%
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(onlb_app).

-behaviour(application).

%-include_lib("kazoo/include/kz_types.hrl").
-include_lib("kazoo_stdlib/include/kz_types.hrl").


-export([start/2, stop/1]).

%%--------------------------------------------------------------------
%% @public
%% @doc Implement the application start behaviour
%%--------------------------------------------------------------------
-spec start(application:start_type(), any()) -> startapp_ret().
start(_Type, _Args) ->
    _ = declare_exchanges(),
    onlb_sup:start_link().

%%--------------------------------------------------------------------
%% @public
%% @doc Implement the application stop behaviour
%%--------------------------------------------------------------------
-spec stop(any()) -> any().
stop(_State) ->
    'ok'.


-spec declare_exchanges() -> 'ok'.
declare_exchanges() ->
    _ = kapi_resource:declare_exchanges(),
    _ = kapi_conf:declare_exchanges(),
    _ = kapi_notifications:declare_exchanges(),
    _ = kapi_money:declare_exchanges(),
    kapi_self:declare_exchanges().
