-module(cb_onlbs).

-export([init/0
         ,allowed_methods/0
         ,resource_exists/0
         ,validate/1
        ]).

-include("/opt/kazoo/applications/crossbar/src/crossbar.hrl").

-include("onlb.hrl").

-spec init() -> 'ok'.
init() ->
    _ = crossbar_bindings:bind(<<"*.allowed_methods.onlbs">>, ?MODULE, 'allowed_methods'),
    _ = crossbar_bindings:bind(<<"*.resource_exists.onlbs">>, ?MODULE, 'resource_exists'),
    _ = crossbar_bindings:bind(<<"*.validate.onlbs">>, ?MODULE, 'validate').

-spec allowed_methods() -> http_methods().
allowed_methods() ->
    [?HTTP_PUT].

-spec resource_exists() -> 'true'.
resource_exists() -> 'true'.

-spec validate(cb_context:context()) -> cb_context:context().
validate(Context) ->
    validate_onlb(Context, cb_context:req_verb(Context)).

-spec validate_onlb(cb_context:context(), http_method()) -> cb_context:context().
validate_onlb(Context, ?HTTP_PUT) ->
    AccountId = cb_context:account_id(Context),
    case cb_context:is_superduper_admin(Context)
         orelse
         (kz_services:get_reseller_id(AccountId) == cb_context:auth_account_id(Context))
    of
        'true' ->
            ReqData = cb_context:req_data(Context),
            Action = kz_json:get_value(<<"action">>, ReqData),
            validate_onlb(Context, kz_term:to_binary(Action), AccountId);
        'false' ->
            cb_context:add_system_error('forbidden', Context)
    end.

validate_onlb(Context, <<"lb_to_kazoo_sync">>, AccountId) ->
    onlb:lb_to_kazoo_sync(AccountId),
    cb_context:set_resp_status(Context, 'success');
validate_onlb(Context, <<"kazoo_to_lb_sync">>, AccountId) ->
    onlb:kazoo_to_lb_sync(AccountId),
    cb_context:set_resp_status(Context, 'success');
validate_onlb(Context, _, _AccountId) ->
    cb_context:add_system_error('forbidden', Context).
    
