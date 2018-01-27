-module(onlb).
-author("Kirill Sysoev <kirill.sysoev@gmail.com>").

-export([add_payment/2
        ,kazoo_to_lb_sync/2
        ,lb_to_kazoo_sync/1
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
            onlb_http:add_payment(AgrmId, Summ, Receipt, Comment, AccountId);
        _ ->
            'ok'
    end.

-spec kazoo_to_lb_sync(ne_binary(), kz_json:object()) -> any().
kazoo_to_lb_sync(AccountId, JObj) ->
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
    onlb_http:soap_create_account(AccountId, Login, kz_binary:rand_hex(7), 1).

-spec lb_to_kazoo_sync(ne_binary()) -> any().
lb_to_kazoo_sync(AccountId) ->
    lb_to_kazoo_sync_account_type(AccountId),
    timer:sleep(300),
    lb_to_kazoo_sync_account_field(<<"name">>, <<"account_name">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"inn">>, <<"account_inn">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"kpp">>, <<"account_kpp">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"ogrn">>, <<"account_ogrn">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"phone">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"fax">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"email">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"mobile">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"gen_dir_u">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"gl_buhg_u">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"kont_person">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"act_on_what">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"pass_sernum">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"pass_no">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"pass_issuedep">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"pass_issueplace">>, AccountId),
    lb_to_kazoo_sync_account_pass(AccountId),
    timer:sleep(300),
    lb_to_kazoo_sync_account_field(<<"birthplace">>, AccountId),
    lb_to_kazoo_sync_account_birthdate(AccountId),
    timer:sleep(300),
    lb_to_kazoo_sync_account_field(<<"abonent_name">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"abonent_surname">>, AccountId),
    lb_to_kazoo_sync_account_field(<<"bank_name">>, [<<"banking_details">>,<<"bank_name">>], AccountId),
    lb_to_kazoo_sync_account_field(<<"branch_bank_name">>, [<<"banking_details">>,<<"branch_bank_name">>], AccountId),
    lb_to_kazoo_sync_account_field(<<"bik">>, [<<"banking_details">>,<<"bik">>], AccountId),
    lb_to_kazoo_sync_account_field(<<"settl">>, [<<"banking_details">>,<<"settlement_account">>], AccountId),
    lb_to_kazoo_sync_account_field(<<"corr">>, [<<"banking_details">>,<<"correspondent_account">>], AccountId),
    sync_addresses(AccountId),
    timer:sleep(300),
    sync_agreements(AccountId),
    timer:sleep(300),
    sync_periodic_fees(AccountId),
    timer:sleep(300),
    lb_to_kazoo_sync_accounts_groups(AccountId),
    timer:sleep(300),
    lb_to_kazoo_sync_accounts_addons_vals_field(<<"'dir_type'">>, <<"dir_type">>, AccountId),
    lb_to_kazoo_sync_accounts_addons_vals_field(<<"'dir_type_rod'">>, <<"dir_type_rod">>, AccountId),
    lb_to_kazoo_sync_accounts_addons_vals_field(<<"'full_type'">>, <<"full_type">>, AccountId),
    lb_to_kazoo_sync_accounts_addons_vals_field(<<"'okato'">>, <<"okato">>, AccountId),
    lb_to_kazoo_sync_accounts_addons_vals_field(<<"'short_name'">>, <<"short_name">>, AccountId),
    lb_to_kazoo_sync_accounts_addons_vals_field(<<"'vlice'">>, <<"vlice">>, AccountId),
    onbill_util:replicate_onbill_doc(AccountId).

update_onbill_doc(Values, AccountId) ->
    DbName = kz_util:format_account_id(AccountId,'encoded'),
    case kz_datamgr:open_doc(DbName, ?ONBILL_DOC) of
        {ok, Doc} ->
            NewDoc = kz_json:set_values(Values, Doc),
            kz_datamgr:ensure_saved(DbName, NewDoc);
        {'error', 'not_found'} ->
            TmpDoc = kz_json:set_values([{<<"_id">>, ?ONBILL_DOC}
                                        ,{<<"pvt_type">>, ?ONBILL_DOC}
                                        ,{<<"pvt_account_id">>, AccountId}
                                        ]
                                       ,kz_json:new()),
            NewDoc = kz_json:set_values(Values, TmpDoc),
            kz_datamgr:ensure_saved(DbName, NewDoc);
        _ ->
            'error'
    end.

lb_to_kazoo_sync_account_field(LbK, AccountId) ->
    lb_to_kazoo_sync_account_field(LbK, LbK, AccountId).

lb_to_kazoo_sync_account_field(LbK, KzK, AccountId) ->
    case onlb_sql:get_field(LbK, <<"accounts">>, AccountId) of
      'undefined' -> 'ok';
      <<>> -> 'ok';
      [] -> 'ok';
      V ->
          update_onbill_doc([{KzK, V}], AccountId),
          timer:sleep(500)
    end.

lb_to_kazoo_sync_account_type(AccountId) ->
    Type =
        case onlb_sql:get_field(<<"type">>, <<"accounts">>, AccountId) of
            2 -> <<"personal">>;
            _ -> <<"corporate">>
        end,
    update_onbill_doc([{<<"customer_type">>, Type}], AccountId).

lb_to_kazoo_sync_account_pass(AccountId) ->
    case onlb_sql:get_field(<<"pass_issuedate">>, <<"accounts">>, AccountId) of
        {Y,M,D} ->
            Values = [
                      {[<<"pass_issuedate">>,<<"year">>], Y}
                     ,{[<<"pass_issuedate">>,<<"month">>], M}
                     ,{[<<"pass_issuedate">>,<<"day">>], D}
                     ],
            update_onbill_doc(Values, AccountId);
        _ -> 'ok'
    end.

lb_to_kazoo_sync_account_birthdate(AccountId) ->
    case onlb_sql:get_field(<<"birthdate">>, <<"accounts">>, AccountId) of
        {Y,M,D} ->
            Values = [
                      {[<<"birthdate">>,<<"year">>], Y}
                     ,{[<<"birthdate">>,<<"month">>], M}
                     ,{[<<"birthdate">>,<<"day">>], D}
                     ],
            update_onbill_doc(Values, AccountId);
        _ -> 'ok'
    end.

sync_addresses(AccountId) ->
    AddressesList = onlb_sql:addresses_data(AccountId),
    case collect_address_data(AddressesList, []) of
        [] -> 'no_addresses';
        Values when is_list(Values) ->
            DbName = kz_util:format_account_id(AccountId,'encoded'),
            case kz_datamgr:open_doc(DbName, ?ONBILL_DOC) of
                {ok, Doc} ->
                    NewDoc = kz_json:set_values(Values, Doc),
                    kz_datamgr:ensure_saved(DbName, NewDoc);
                _ ->
                    'open_doc_error'
            end;
        _ ->
            'error'
    end.

sync_agreements(AccountId) ->
    AgrmsList = onlb_sql:agreements_data(AccountId),
    case collect_agreements_data(AgrmsList, []) of
        [] -> 'no_agreements';
        Values when is_list(Values) ->
            DbName = kz_util:format_account_id(AccountId,'encoded'),
            case kz_datamgr:open_doc(DbName, ?ONBILL_DOC) of
                {ok, Doc} ->
                    NewDoc = kz_json:set_values(Values, Doc),
                    kz_datamgr:ensure_saved(DbName, NewDoc);
                _ ->
                    'open_doc_error'
            end;
        _ ->
            'error'
    end.

sync_periodic_fees(AccountId) ->
    remove_periodic_fees(AccountId),
    FeesList = onlb_sql:get_periodic_fees(AccountId),
    add_periodic_fees(FeesList, AccountId).

remove_periodic_fees(AccountId) ->
    Services = kz_services:delete_service_plan(<<"voip_service_plan">>, kz_services:fetch(AccountId)),
    kz_services:save(kz_services:add_service_plan(<<"onnet_periodic_fees">>, Services)),
    DbName = kz_util:format_account_id(AccountId, 'encoded'),
    case kz_datamgr:get_result_ids(DbName, <<"periodic_fees/crossbar_listing">>, []) of
        {'ok', Ids} ->
            kz_datamgr:del_docs(DbName, Ids);
        {'error', _R} ->
            lager:debug("unable to get periodic_fees docs of ~s: ~p", [AccountId, _R])
    end.

add_periodic_fees([], _AccountId) ->
    'ok';
add_periodic_fees([[<<"phone_line_649">>, Qty]|FeesLeft], AccountId) ->
    DbName = kz_util:format_account_id(AccountId, 'encoded'),
    case kz_datamgr:open_doc(DbName, <<"limits">>) of
        {ok, Doc} ->
            NewDoc = kz_json:set_value(<<"twoway_trunks">>, kz_term:to_integer(Qty), Doc),
            kz_datamgr:ensure_saved(DbName, NewDoc),
            AccountId;
        {'error', 'not_found'} ->
            Values =
                props:filter_undefined(
                    [{<<"_id">>, <<"limits">>}
                    ,{<<"pvt_type">>, <<"limits">>}
                    ,{<<"twoway_trunks">>, kz_term:to_integer(Qty)}
                    ]),
            NewDoc = kz_json:set_values(Values ,kz_json:new()),
            kz_datamgr:ensure_saved(DbName, NewDoc),
            AccountId;
        _ ->
            'onbill_data_not_added'
    end,
    add_periodic_fees(FeesLeft, AccountId);
add_periodic_fees([[FeeId, Qty, From, Till]|FeesLeft], AccountId) ->
    DbName = kz_util:format_account_id(AccountId, 'encoded'),
    ServiceStarts = calendar:datetime_to_gregorian_seconds(From),
    ServiceEnds = calendar:datetime_to_gregorian_seconds(Till),
    Values =
        props:filter_undefined(
            [{<<"_id">>, kz_datamgr:get_uuid()}
            ,{<<"pvt_type">>, <<"periodic_fee">>}
            ,{<<"service_id">>, FeeId}
            ,{<<"quantity">>, Qty}
            ,{<<"service_starts">>, ServiceStarts}
            ,{<<"service_ends">>, ServiceEnds}
            ]),
    kz_datamgr:save_doc(DbName,kz_json:from_list(Values)),
    add_periodic_fees(FeesLeft, AccountId).

lb_to_kazoo_sync_accounts_groups(AccountId) ->
    case onlb_sql:accounts_groups(AccountId) of
        Groups when is_list(Groups) andalso length(Groups) > 0->
            Values =
                [{[<<"accounts_groups">>,kapps_config:get_binary(<<"onlb">>, [<<"accounts_groups">>, kz_term:to_binary(Id)])], 'true'}
                 || [Id] <- Groups
                , kapps_config:get_binary(<<"onlb">>, [<<"accounts_groups">>, kz_term:to_binary(Id)]) /= 'undefined'
                ],
            update_onbill_doc(Values, AccountId);
        _ -> 'ok'
    end.

lb_to_kazoo_sync_accounts_addons_vals_field(LbF, KzK, AccountId) ->
    case onlb_sql:get_field(<<"str_value">>, {<<"name">>, LbF}, <<"accounts_addons_vals">>, AccountId) of
      'undefined' -> 'ok';
      <<>> -> 'ok';
      [] -> 'ok';
      V ->
          update_onbill_doc([{KzK, V}], AccountId),
          timer:sleep(500)
    end.

collect_agreements_data([], Acc) ->
    Acc;
collect_agreements_data([[OperId, AgrmNumber, {Y,M,D}]|Tail], Acc) ->
    case kapps_config:get_binary(<<"onlb">>, [<<"opers_to_carriers">>, kz_term:to_binary(OperId)]) of
        'undefined' ->
            collect_agreements_data(Tail, Acc);
        CarrierId ->
            Values =
                [{[<<"agrm">>, CarrierId, <<"number">>], AgrmNumber}
                ,{[<<"agrm">>, CarrierId, <<"date_json">>, <<"day">>], D}
                ,{[<<"agrm">>, CarrierId, <<"date_json">>, <<"month">>], M}
                ,{[<<"agrm">>, CarrierId, <<"date_json">>, <<"year">>], Y}
                ],
            collect_agreements_data(Tail, Acc ++ Values)
    end;
collect_agreements_data(_, Acc) ->
    Acc.

collect_address_data([], Acc) ->
    Acc;
collect_address_data([[AddrId, AddrLine]|Tail], Acc) ->
    case kapps_config:get_binary(<<"onlb">>, [<<"address_types">>, kz_term:to_binary(AddrId)]) of
        'undefined' ->
            collect_agreements_data(Tail, Acc);
        AddrType ->
            case binary:split(AddrLine, [<<",">>], [global]) of
                [A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11] -> ok;
                [A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, _, A11] -> ok
            end,
            AA4 = case A2 == A4 of
                      'true' -> <<>>;
                      'false' -> A4
                  end,
            Values =
                [{[<<"address">>, AddrType, <<"line3">>], maybe_format_address_element([A8, A9, A10], A7)}
                ,{[<<"address">>, AddrType, <<"line2">>], maybe_format_address_element([AA4, A5, A6], A3)}
                ,{[<<"address">>, AddrType, <<"line1">>], maybe_format_address_element([A1, A2], A11)}
                ],
            collect_address_data(Tail, Acc ++ Values)
    end;
collect_address_data(_, Acc) ->
    Acc.

-spec maybe_format_address_element(any(), any()) -> 'ok'.
maybe_format_address_element([], Acc) ->
    Acc;
maybe_format_address_element([<<>>], Acc) ->
    Acc;
maybe_format_address_element([H], <<>>) ->
    H;
maybe_format_address_element([H], Acc) ->
    <<Acc/binary, ", ", H/binary>>;
maybe_format_address_element([<<>>|T], Acc) ->
    maybe_format_address_element(T, Acc);
maybe_format_address_element([H|T], Acc) when Acc == <<>> ->
    maybe_format_address_element(T, <<H/binary>>);
maybe_format_address_element([H|T], Acc) ->
    maybe_format_address_element(T, <<Acc/binary, ", ", H/binary>>).

