-module(onlb).
-author("Kirill Sysoev <kirill.sysoev@gmail.com>").

-export([add_payment/2
        ,sync_onbill_lb_info/2
        ]).

-include_lib("onlb.hrl").

-spec add_payment(ne_binary(), kz_json:object()) -> any().
add_payment(AccountId, JObj) ->
    EncodedDb = kz_json:get_value(<<"Database">>, JObj),
    DocId = kz_json:get_value(<<"ID">>, JObj),
    {'ok', Doc} = kz_datamgr:open_doc(EncodedDb, DocId),
    case kz_json:get_binary_value(<<"pvt_reason">>, Doc) of
        <<"wire_transfer">> ->
            AgrmId = onlb_sql:get_main_agrm_id(AccountId),
            Summ = wht_util:units_to_dollars(kz_json:get_integer_value(<<"pvt_amount">>, Doc, 0)),
            Receipt = kz_json:get_binary_value(<<"_id">>, Doc, <<>>),
            Comment = kz_json:get_binary_value(<<"description">>, Doc, <<>>),
            lb_http:add_payment(AgrmId, Summ, Receipt, Comment, AccountId);
        _ ->
            'ok'
    end.

-spec sync_onbill_lb_info(ne_binary(), kz_json:object()) -> any().
sync_onbill_lb_info(AccountId, JObj) ->
    EncodedDb = kz_json:get_value(<<"Database">>, JObj),
    {'ok', Doc} = kz_datamgr:open_doc(EncodedDb, <<"onbill">>),
    case onlb_sql:lbuid_by_uuid(AccountId) of
        'undefined' ->
            create_lb_account(AccountId, Doc),
            timer:sleep(1000),
            onlb_sql:update_lb_account(onlb_sql:lbuid_by_uuid(AccountId), AccountId, Doc);
        UID ->
            onlb_sql:update_lb_account(UID, AccountId, Doc)
    end.

-spec create_lb_account(ne_binary(), kz_json:object()) -> any().
create_lb_account(AccountId, _Doc) ->
    {'ok', AccountJObj} = kz_account:fetch(AccountId),
    [Login|_] = binary:split(kz_account:realm(AccountJObj), <<".">>),
    lb_http:soap_create_account(AccountId, Login, kz_binary:rand_hex(7), 1).
