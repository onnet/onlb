-module(onlb_handlers).

-export([handle_doc_created/2
        ,handle_doc_edited/2
        ,handle_logger/2
        ]).

-include("onlb.hrl").

-spec handle_doc_created(kz_json:object(), kz_proplist()) -> any().
handle_doc_created(JObj, _Props) ->
    AccountId = kz_json:get_value(<<"Account-ID">>, JObj),
    case (AccountId /= 'undefined')
         andalso kz_datamgr:db_exists(kz_util:format_account_id(AccountId, 'encoded'))
         andalso not kapps_util:is_master_account(AccountId)
    of
        'true' ->
            handle_doc_created(kz_json:get_value(<<"Type">>, JObj), AccountId, JObj);
        'false' ->
            'ok'
    end.

handle_doc_created(<<"credit">>, AccountId, _JObj) ->
    _ = kz_util:spawn(fun onlb:add_payment/2, [AccountId, _JObj]);
handle_doc_created(_, _, _) ->
    'ok'.

-spec handle_doc_edited(kz_json:object(), kz_proplist()) -> any().
handle_doc_edited(JObj, _Props) ->
    AccountId = kz_json:get_value(<<"Account-ID">>, JObj),
    case (AccountId /= 'undefined')
         andalso kz_datamgr:db_exists(kz_util:format_account_id(AccountId, 'encoded'))
         andalso not kapps_util:is_master_account(AccountId)
    of
        'true' ->
            handle_doc_edited(kz_json:get_value(<<"Type">>, JObj), AccountId, JObj);
        'false' ->
            'ok'
    end.

handle_doc_edited('undefined', _AccountId, JObj) ->
    case kz_json:get_value(<<"ID">>, JObj) of
        <<"onbill">> ->
    %        _ = kz_util:spawn(fun onlb:kazoo_to_lb_sync/2, [AccountId, JObj]);
            'ok';
        _ ->
            'ok'
    end;
handle_doc_edited(_, _, _) ->
    'ok'.

-spec handle_logger(kz_json:object(), kz_proplist()) -> any().
handle_logger(_JObj, _Props) ->
    'ok'.
