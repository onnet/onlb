-module(kt_onlb).
%% behaviour: tasks_provider

-export([init/0
        ,help/1, help/2, help/3
        ,output_header/1
        ]).

%% Verifiers
-export([
        ]).

%% Appliers
-export([balance_comparison/2
        ,credit_comparison/2
        ,usage_comparison/2
        ,sync_bom_balance/2
        ,import_periodic_fees/3
        ,import_accounts/3
        ,import_onbill_data1/3
        ,is_allowed/1
        ]).

%% Dev temp
-export([fetch_transactions_docs/1
        ,filter_transactions/2
        ,summ_transactions/1
        ]).

-include_lib("tasks/src/tasks.hrl").
-include_lib("kazoo_services/include/kz_service.hrl").
-include("onlb.hrl").

-define(CATEGORY, "onlb").
-define(ACTIONS, [<<"balance_comparison">>
                 ,<<"credit_comparison">>
                 ,<<"usage_comparison">>
                 ,<<"sync_bom_balance">>
                 ,<<"import_periodic_fees">>
                 ,<<"import_accounts">>
                 ,<<"import_onbill_data1">>
                 ]).

-define(IMPORT_PERIODIC_FEES_DOC_FIELDS
       ,[<<"account_id">>
        ,<<"service_id">>
        ,<<"quantity">>
        ]).

-define(IMPORT_PERIODIC_FEES_MANDATORY_FIELDS
       ,[<<"account_id">>
        ,<<"service_id">>
        ,<<"quantity">>
        ]).


-define(IMPORT_ACCOUNTS_DOC_FIELDS
       ,[<<"account_name">>
        ,<<"realm">>
        ,<<"users">>
        ]).

-define(IMPORT_ACCOUNTS_MANDATORY_FIELDS
       ,[<<"account_name">>
        ]).

-define(IMPORT_ONBILL_DATA1
       ,[<<"account_id">>
        ,<<"account_name">>
        ,<<"account_inn">>
        ,<<"account_kpp">>
        ,<<"prepaid">>
        ,<<"billing_address_line1">>
        ,<<"billing_address_line2">>
        ,<<"billing_address_line3">>
        ,<<"agrm_number">>
        ,<<"agrm_date">>
        ,<<"agrm_type_id">>
        ]).

-define(IMPORT_MANDATORY_ONBILL_DATA1
       ,[<<"account_id">>
        ]).

-define(ACCOUNT_REALM_SUFFIX
       ,kapps_config:get_binary(<<"crossbar.accounts">>, <<"account_realm_suffix">>, <<"sip.onnet.su">>)).

-define(MK_USER,
    {[{<<"call_forward">>,
       {[{<<"substitute">>,false},
         {<<"enabled">>,false},
         {<<"require_keypress">>,false},
         {<<"keep_caller_id">>,false},
         {<<"direct_calls_only">>,false}]}},
      {<<"enabled">>, 'true'},
      {<<"priv_level">>,<<"user">>},
      {<<"vm_to_email_enabled">>,true},
      {<<"fax_to_email_enabled">>,true},
      {<<"verified">>,false},
      {<<"timezone">>,<<"UTC">>},
      {<<"record_call">>,false}
     ]}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> 'ok'.
init() ->
    _ = tasks_bindings:bind(<<"tasks.help">>, ?MODULE, 'help'),
    _ = tasks_bindings:bind(<<"tasks."?CATEGORY".output_header">>, ?MODULE, 'output_header'),
    tasks_bindings:bind_actions(<<"tasks."?CATEGORY>>, ?MODULE, ?ACTIONS).

-spec output_header(ne_binary()) -> kz_csv:row().
output_header(<<"balance_comparison">>) ->
    [<<"account_id">>
    ,<<"account_name">>
    ,<<"bom_kazoo_balance">>
    ,<<"bom_lb_balance">>
    ,<<"bom_difference">>
    ,<<"current_kazoo_balance">>
    ,<<"current_lb_balance">>
    ,<<"current_difference">>
    ];

output_header(<<"credit_comparison">>) ->
    [<<"account_id">>
    ,<<"account_name">>
    ,<<"kazoo_credit">>
    ,<<"lb_credit">>
    ,<<"difference">>
    ];

output_header(<<"usage_comparison">>) ->
    [<<"account_id">>
    ,<<"account_name">>
    ,<<"prevmonth_kazoo_usage">>
    ,<<"prevmonth_lb_usage">>
    ,<<"prevmonth_difference">>
    ,<<"kazoo_usage">>
    ,<<"lb_usage">>
    ,<<"difference">>
    ];

output_header(<<"sync_bom_balance">>) ->
    [<<"account_id">>
    ,<<"account_name">>
    ,<<"account_type">>
    ,<<"balance_set">>
    ].

-spec help(kz_json:object()) -> kz_json:object().
help(JObj) -> help(JObj, <<?CATEGORY>>).

-spec help(kz_json:object(), ne_binary()) -> kz_json:object().
help(JObj, <<?CATEGORY>>=Category) ->
    lists:foldl(fun(Action, J) -> help(J, Category, Action) end, JObj, ?ACTIONS).

-spec help(kz_json:object(), ne_binary(), ne_binary()) -> kz_json:object().
help(JObj, <<?CATEGORY>>=Category, Action) ->
    kz_json:set_value([Category, Action], kz_json:from_list(action(Action)), JObj).

-spec action(ne_binary()) -> kz_proplist().
action(<<"balance_comparison">>) ->
    [{<<"description">>, <<"compare Kazoo and LanBilling balances">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"credit_comparison">>) ->
    [{<<"description">>, <<"compare Kazoo and LanBilling current month total credit">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"usage_comparison">>) ->
    [{<<"description">>, <<"compare Kazoo and LanBilling current month charges">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"sync_bom_balance">>) ->
    [{<<"description">>, <<"sync Kazoo bom balance to LanBilling's one">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"import_periodic_fees">>) ->
    Mandatory = ?IMPORT_PERIODIC_FEES_MANDATORY_FIELDS,
    Optional = ?IMPORT_PERIODIC_FEES_DOC_FIELDS -- Mandatory,

    [{<<"description">>, <<"Bulk-import using periodic fees list">>}
    ,{<<"doc">>, <<"Assigning periodic fees from file">>}
    ,{<<"expected_content">>, <<"text/csv">>}
    ,{<<"mandatory">>, Mandatory}
    ,{<<"optional">>, Optional}
    ];

action(<<"import_accounts">>) ->
    Mandatory = ?IMPORT_ACCOUNTS_MANDATORY_FIELDS,
    Optional = ?IMPORT_ACCOUNTS_DOC_FIELDS -- Mandatory,

    [{<<"description">>, <<"Bulk-create accounts using account_names list">>}
    ,{<<"doc">>, <<"Creates accounts from file">>}
    ,{<<"expected_content">>, <<"text/csv">>}
    ,{<<"mandatory">>, Mandatory}
    ,{<<"optional">>, Optional}
    ];

action(<<"import_onbill_data1">>) ->
    Mandatory = ?IMPORT_MANDATORY_ONBILL_DATA1,
    Optional = ?IMPORT_ONBILL_DATA1 -- Mandatory,

    [{<<"description">>, <<"Bulk-import accounts company data for invoice generating">>}
    ,{<<"doc">>, <<"Imports onbill data from file">>}
    ,{<<"expected_content">>, <<"text/csv">>}
    ,{<<"mandatory">>, Mandatory}
    ,{<<"optional">>, Optional}
    ].

%%% Verifiers


%%% Appliers

-spec balance_comparison(kz_tasks:extra_args(), kz_tasks:iterator()) -> kz_tasks:iterator().
balance_comparison(#{account_id := AccountId}, init) ->
    {'ok', get_children(AccountId)};
balance_comparison(_, []) -> stop;
balance_comparison(_, [SubAccountId | DescendantsIds]) ->
    {'ok', JObj} = kz_account:fetch(SubAccountId),
    {{Y,M,_}, _ } = calendar:gregorian_seconds_to_datetime(kz_time:current_tstamp()),
    BOM_KazooBalance =
       case onbill_util:day_start_balance_dollars(SubAccountId, Y, M, 1) of
           {'error', _} -> 'error';
           BomBalance -> BomBalance
       end,
    BOM_LbBalance = onlb_sql:bom_balance(SubAccountId),
    BOM_Difference =
        try kz_term:to_float(BOM_KazooBalance) - kz_term:to_float(BOM_LbBalance) catch _:_ -> 'math_error' end,
    CurrentKazooBalance = onbill_util:current_account_dollars(SubAccountId),
    CurrentLbBalance = onlb_sql:account_balance(SubAccountId),
    CurrentDifference =
        try kz_term:to_float(CurrentKazooBalance) - kz_term:to_float(CurrentLbBalance) catch _:_ -> 'math_error' end,
    {[SubAccountId
     ,kz_account:name(JObj)
     ,try onbill_util:price_round(BOM_KazooBalance) catch _:_ -> BOM_KazooBalance end
     ,try onbill_util:price_round(BOM_LbBalance) catch _:_ -> BOM_LbBalance end
     ,try onbill_util:price_round(BOM_Difference) catch _:_ -> BOM_Difference end
     ,try onbill_util:price_round(CurrentKazooBalance) catch _:_ -> CurrentKazooBalance end
     ,try onbill_util:price_round(CurrentLbBalance) catch _:_ -> CurrentLbBalance end
     ,try onbill_util:price_round(CurrentDifference) catch _:_ -> CurrentDifference end
     ], DescendantsIds}.

-spec credit_comparison(kz_tasks:extra_args(), kz_tasks:iterator()) -> kz_tasks:iterator().
credit_comparison(#{account_id := AccountId}, init) ->
    {'ok', get_children(AccountId)};
credit_comparison(_, []) -> stop;
credit_comparison(_, [SubAccountId | DescendantsIds]) ->
    {'ok', JObj} = kz_account:fetch(SubAccountId),
    KazooCredit = wht_util:units_to_dollars(summ_transactions(filter_transactions(<<"credit">>, fetch_transactions_docs(SubAccountId)))),
    LbCredit = onlb_sql:curr_month_credit(SubAccountId),
    Difference =
        try kz_term:to_float(KazooCredit) - kz_term:to_float(LbCredit) catch _:_ -> 'math_error' end,
    {[SubAccountId
     ,kz_account:name(JObj)
     ,try onbill_util:price_round(KazooCredit) catch _:_ -> KazooCredit end
     ,try onbill_util:price_round(LbCredit) catch _:_ -> LbCredit end
     ,try onbill_util:price_round(Difference) catch _:_ -> Difference end
     ], DescendantsIds}.

-spec usage_comparison(kz_tasks:extra_args(), kz_tasks:iterator()) -> kz_tasks:iterator().
usage_comparison(#{account_id := AccountId}, init) ->
    {'ok', get_children(AccountId)};
usage_comparison(_, []) -> stop;
usage_comparison(_, [SubAccountId | DescendantsIds]) ->
    {'ok', JObj} = kz_account:fetch(SubAccountId),
    {{Y,M,_}, _ } = calendar:gregorian_seconds_to_datetime(kz_time:current_tstamp()),
    {PY, PM} = onbill_util:prev_month(Y, M),
    PrevMonthKazooUsage = wht_util:units_to_dollars(summ_transactions(filter_transactions(<<"debit">>, fetch_transactions_docs(SubAccountId, PY, PM)))),
    PrevMonthLbUsage = onlb_sql:calc_prev_month_exp(SubAccountId),
    PrevMonthDifference =
        try kz_term:to_float(PrevMonthKazooUsage) - kz_term:to_float(PrevMonthLbUsage) catch _:_ -> 'math_error' end,
    KazooUsage = wht_util:units_to_dollars(summ_transactions(filter_transactions(<<"debit">>, fetch_transactions_docs(SubAccountId)))),
    LbUsage = onlb_sql:calc_curr_month_exp(SubAccountId),
    Difference =
        try kz_term:to_float(KazooUsage) - kz_term:to_float(LbUsage) catch _:_ -> 'math_error' end,
    {[SubAccountId
     ,kz_account:name(JObj)
     ,try onbill_util:price_round(PrevMonthKazooUsage) catch _:_ -> PrevMonthKazooUsage end
     ,try onbill_util:price_round(PrevMonthLbUsage) catch _:_ -> PrevMonthLbUsage end
     ,try onbill_util:price_round(PrevMonthDifference) catch _:_ -> PrevMonthDifference end
     ,try onbill_util:price_round(KazooUsage) catch _:_ -> KazooUsage end
     ,try onbill_util:price_round(LbUsage) catch _:_ -> LbUsage end
     ,try onbill_util:price_round(Difference) catch _:_ -> Difference end
     ], DescendantsIds}.

-spec sync_bom_balance(kz_tasks:extra_args(), kz_tasks:iterator()) -> kz_tasks:iterator().
sync_bom_balance(#{account_id := AccountId}, init) ->
    {'ok', get_children(AccountId)};
sync_bom_balance(_, []) -> stop;
sync_bom_balance(_, [SubAccountId | DescendantsIds]) ->
    {'ok', JObj} = kz_account:fetch(SubAccountId),
    AccountType = account_type(SubAccountId),
    Balance =
        case AccountType of
            <<"POS">> ->
                LB_BOM_Balance = onlb_sql:calc_prev_month_exp(SubAccountId),
                try -1 * kz_term:to_integer(wht_util:dollars_to_units(LB_BOM_Balance)) catch _:_ -> LB_BOM_Balance end;
            <<"PRE">> ->
                LB_BOM_Balance = onlb_sql:bom_balance(SubAccountId),
                try kz_term:to_integer(wht_util:dollars_to_units(LB_BOM_Balance)) catch _:_ -> LB_BOM_Balance end
        end,
    Result = set_bom_balance(Balance, SubAccountId),
    {[SubAccountId
     ,kz_account:name(JObj)
     ,AccountType
     ,try onbill_util:price_round(wht_util:units_to_dollars(Result)) catch _:_ -> Result end
     ], DescendantsIds}.

-spec import_periodic_fees(kz_tasks:extra_args(), kz_tasks:iterator(), kz_tasks:args()) ->
                    {kz_tasks:return(), sets:set()}.
import_periodic_fees(ExtraArgs=#{account_id := ResellerId}, init, Args) ->
    remove_periodic_fees_from_db(get_descendants(ResellerId)),
    kz_datamgr:suppress_change_notice(),
    IterValue = sets:new(),
    import_periodic_fees(ExtraArgs, IterValue, Args);
import_periodic_fees(_
                    ,_
                    ,#{<<"account_id">> := AccountId
                      ,<<"service_id">> := <<"phone_line_649">>
                      ,<<"quantity">> := Quantity
                      }
      ) ->
    DbName = kz_util:format_account_id(AccountId, 'encoded'),
    case kz_datamgr:open_doc(DbName, <<"limits">>) of
        {ok, Doc} ->
            NewDoc = kz_json:set_value(<<"twoway_trunks">>, kz_term:to_integer(Quantity), Doc),
            kz_datamgr:ensure_saved(DbName, NewDoc),
            AccountId;
        {'error', 'not_found'} ->
            Values =
                props:filter_undefined(
                    [{<<"_id">>, <<"limits">>}
                    ,{<<"pvt_type">>, <<"limits">>}
                    ,{<<"twoway_trunks">>, kz_term:to_integer(Quantity)}
                    ]),
            NewDoc = kz_json:set_values(Values ,kz_json:new()),
            kz_datamgr:ensure_saved(DbName, NewDoc),
            AccountId;
        _ ->
            'onbill_data_not_added'
    end;
import_periodic_fees(_
                    ,_
                    ,#{<<"account_id">> := AccountId
                      ,<<"service_id">> := ServiceId
                      ,<<"quantity">> := Quantity
                      }
      ) ->
    DbName = kz_util:format_account_id(AccountId, 'encoded'),
    ServiceStarts = calendar:datetime_to_gregorian_seconds({onbill_util:period_start_date(AccountId), {0,0,0}}),
    Values =
        props:filter_undefined(
            [{<<"_id">>, kz_datamgr:get_uuid()}
            ,{<<"pvt_type">>, <<"periodic_fee">>}
            ,{<<"service_id">>, ServiceId}
            ,{<<"quantity">>, Quantity}
            ,{<<"service_starts">>, ServiceStarts}
            ]),
    kz_datamgr:save_doc(DbName,kz_json:from_list(Values)),
    AccountId.

-spec import_accounts(kz_tasks:extra_args(), kz_tasks:iterator(), kz_tasks:args()) ->
                    {kz_tasks:return(), sets:set()}.
import_accounts(ExtraArgs, init, Args) ->
    kz_datamgr:suppress_change_notice(),
    IterValue = sets:new(),
    import_accounts(ExtraArgs, IterValue, Args);
import_accounts(#{account_id := ResellerId
        ,auth_account_id := _AuthAccountId
        }
      ,_AccountIds
      ,_Args=#{<<"account_name">> := AccountName
             ,<<"users">> := UserString
             }
      ) ->
    Realm = <<AccountName/binary, ".", (?ACCOUNT_REALM_SUFFIX)/binary>>,
    Context = create_account(ResellerId, AccountName, Realm),
    case cb_context:resp_status(Context) of
        'success' ->
            kz_util:spawn(fun cb_onbill_signup:create_default_callflow/1, [Context]),
            RespData = cb_context:resp_data(Context),
            AccountId = kz_json:get_value(<<"id">>, RespData),
            case UserString of
                'undefined' ->
                    AccountId;
                _ ->
                    Users = binary:split(re:replace(UserString, "\\s+", "", [global,{return,binary}])
                                        ,[<<",">>,<<";">>]),
                    create_users(AccountId, Users, Context),
                    AccountId
            end;
        _ ->
            'account_not_created'
    end.

-spec import_onbill_data1(kz_tasks:extra_args(), kz_tasks:iterator(), kz_tasks:args()) ->
                    {kz_tasks:return(), sets:set()}.
import_onbill_data1(ExtraArgs, init, Args) ->
    case is_allowed(ExtraArgs) of
        'true' ->
            lager:info("import_onbill_data1 is allowed, continuing"),
            kz_datamgr:suppress_change_notice(),
            IterValue = sets:new(),
            import_onbill_data1(ExtraArgs, IterValue, Args);
        'false' ->
            lager:warning("import_onbill_data1 is forbidden for account ~s, auth account ~s"
                         ,[maps:get('account_id', ExtraArgs)
                          ,maps:get('auth_account_id', ExtraArgs)
                          ]
                         ),
            {<<"task execution is forbidden">>, 'stop'}
    end;
import_onbill_data1(#{account_id := _ResellerId
        ,auth_account_id := _AuthAccountId
        }
      ,_AccountIds
      ,_Args=#{<<"account_id">> := AccountId
              ,<<"account_name">> := AccountName
              ,<<"account_inn">> := AccountINN
              ,<<"account_kpp">> := AccountKPP
              ,<<"prepaid">> := _Prepaid
              ,<<"billing_address">> := BillingAddress
              ,<<"agrm_number">> := AgrmNumber
              ,<<"agrm_date">> := AgrmDate
              ,<<"agrm_type_id">> := AgrmTypeId
              }
      ) ->
    case binary:split(BillingAddress, [<<"^*^">>], [global]) of
        [A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11] -> ok;
        [A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, _, A11] -> ok
    end,
    AA4 = case A2 == A4 of
              'true' -> <<>>;
              'false' -> A4
          end,
    Values = props:filter_empty(
        [{<<"_id">>, ?ONBILL_DOC}
        ,{<<"pvt_type">>, ?ONBILL_DOC}
        ,{<<"pvt_account_id">>, AccountId}
        ,{<<"account_name">>, AccountName}
        ,{<<"account_inn">>, AccountINN}
        ,{<<"account_kpp">>, AccountKPP}
        ,{[<<"billing_address">>,<<"line1">>]
         ,maybe_format_address_element([A1, A2], A11)
         }
        ,{[<<"billing_address">>,<<"line2">>]
         ,maybe_format_address_element([AA4, A5, A6], A3)
         }
        ,{[<<"billing_address">>,<<"line3">>]
         ,maybe_format_address_element([A8, A9, A10], A7)
         }
        ] ++ agrm_vals(AgrmNumber, AgrmDate, AgrmTypeId)),
    DbName = kz_util:format_account_id(AccountId,'encoded'),
    case kz_datamgr:open_doc(DbName, ?ONBILL_DOC) of
        {ok, Doc} ->
            NewDoc = kz_json:set_values(Values, Doc),
            kz_datamgr:ensure_saved(DbName, NewDoc),
            AccountId;
        {'error', 'not_found'} ->
            NewDoc = kz_json:set_values(Values ,kz_json:new()),
            kz_datamgr:ensure_saved(DbName, NewDoc),
            AccountId;
        _ ->
            'onbill_data_not_added'
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_descendants(ne_binary()) -> ne_binaries().
get_descendants(AccountId) ->
    ViewOptions = [{'startkey', [AccountId]}
                  ,{'endkey', [AccountId, kz_json:new()]}
                  ],
    case kz_datamgr:get_results(?KZ_ACCOUNTS_DB, <<"accounts/listing_by_descendants">>, ViewOptions) of
        {'ok', JObjs} -> [kz_doc:id(JObj) || JObj <- JObjs];
        {'error', _R} ->
            lager:debug("unable to get descendants of ~s: ~p", [AccountId, _R]),
            []
    end.

-spec get_children(ne_binary()) -> ne_binaries().
get_children(AccountId) ->
    ViewOptions = [{'startkey', [AccountId]}
                  ,{'endkey', [AccountId, kz_json:new()]}
                  ],
    case kz_datamgr:get_results(?KZ_ACCOUNTS_DB, <<"accounts/listing_by_children">>, ViewOptions) of
        {'ok', JObjs} -> [kz_doc:id(JObj) || JObj <- JObjs];
        {'error', _R} ->
            lager:debug("unable to get descendants of ~s: ~p", [AccountId, _R]),
            []
    end.

-spec is_allowed(kz_tasks:extra_args()) -> boolean().
is_allowed(ExtraArgs) ->
    AuthAccountId = maps:get('auth_account_id', ExtraArgs),
    AccountId = maps:get('account_id', ExtraArgs),
    {'ok', AccountDoc} = kz_account:fetch(AccountId),
    {'ok', AuthAccountDoc} = kz_account:fetch(AuthAccountId),
    kz_util:is_in_account_hierarchy(AuthAccountId, AccountId, 'true')
        andalso kz_account:is_reseller(AccountDoc)
        orelse kz_account:is_superduper_admin(AuthAccountDoc).

create_account(ResellerId, AccountName, Realm) ->
    Tree = crossbar_util:get_tree(ResellerId) ++ [ResellerId],
    Props = [{<<"pvt_type">>, kz_account:type()}
            ,{<<"name">>, AccountName}
            ,{<<"realm">>, Realm}
            ,{<<"timezone">>,<<"Europe/Moscow">>}
            ,{<<"language">>,<<"ru-ru">>}
            ,{<<"pvt_tree">>, Tree}
            ],
    Ctx1 = cb_context:set_account_id(cb_context:new(), ResellerId),
    Ctx2 = cb_context:set_doc(Ctx1, kz_json:set_values(Props, kz_json:new())),
    cb_accounts:put(Ctx2).

create_users(_AccountId, [], _Context) -> 'ok';
create_users(AccountId, [UserName|Users], Context) -> 
    UserPassword = kz_binary:rand_hex(10),
    Props = props:filter_empty([
         {[<<"username">>], UserName}
        ,{[<<"first_name">>], <<"Firstname">>}
        ,{[<<"last_name">>], <<"Surname">>}
        ,{[<<"email">>], UserName}
        ,{[<<"password">>], UserPassword}
        ,{[<<"timezone">>],<<"Europe/Moscow">>}
        ,{[<<"priv_level">>], <<"admin">>}
        ]),
    UserData = kz_json:set_values(Props, ?MK_USER),
    Ctx1 = cb_context:set_account_id(Context, AccountId),
    Ctx2 = cb_context:set_doc(Ctx1, UserData),
    Ctx3 = cb_context:set_req_data(Ctx2, UserData),
    Ctx4 = cb_users_v1:put(Ctx3),
    send_email(Ctx4),
    timer:sleep(1000),
    create_users(AccountId, Users, Context).

-spec send_email(cb_context:context()) -> 'ok'.
send_email(Context) ->
    Doc = cb_context:doc(Context),
    ReqData = cb_context:req_data(Context),
    Req = [{<<"Account-ID">>, cb_context:account_id(Context)}
          ,{<<"User-ID">>, kz_doc:id(Doc)}
          ,{<<"Password">>, kz_json:get_value(<<"password">>, ReqData)}
           | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
          ],
    kapps_notify_publisher:cast(Req, fun kapi_notifications:publish_new_user/1).

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

format_agrm_date(<<YYYY:4/binary, "-", MM:2/binary, "-", DD:2/binary>>) ->
    <<DD/binary, ".", MM/binary, ".", YYYY/binary>>;
format_agrm_date(AgrmDate) ->
    AgrmDate.

agrm_vals(_, _, 'undefined') ->
    [];
agrm_vals(<<"I0#", AgrmNumber/binary>>, AgrmDate, AgrmTypeId) ->
    agrm_vals(AgrmNumber, AgrmDate, AgrmTypeId);
agrm_vals(<<"0WG#", AgrmNumber/binary>>, AgrmDate, AgrmTypeId) ->
    agrm_vals(AgrmNumber, AgrmDate, AgrmTypeId);
agrm_vals(AgrmNumber, AgrmDate, <<"1">>) ->
    [{[<<"agrm">>,<<"onnet">>,<<"number">>], AgrmNumber}
    ,{[<<"agrm">>,<<"onnet">>,<<"date">>], format_agrm_date(AgrmDate)}];
agrm_vals(AgrmNumber, AgrmDate, <<"2">>) ->
    [{[<<"agrm">>,<<"beeline_spb">>,<<"number">>], <<"I0#", AgrmNumber/binary>>}
    ,{[<<"agrm">>,<<"beeline_spb">>,<<"date">>], format_agrm_date(AgrmDate)}];
agrm_vals(AgrmNumber, AgrmDate, <<"3">>) ->
    [{[<<"agrm">>,<<"beeline_msk">>,<<"number">>], <<"0WG#", AgrmNumber/binary>>}
    ,{[<<"agrm">>,<<"beeline_msk">>,<<"date">>], format_agrm_date(AgrmDate)}].

remove_periodic_fees_from_db([]) ->
    'ok';
remove_periodic_fees_from_db([DescendantId | DescendantsIds]) ->
    Services = kz_services:delete_service_plan(<<"voip_service_plan">>, kz_services:fetch(DescendantId)),
    kz_services:save(kz_services:add_service_plan(<<"onnet_periodic_fees">>, Services)),
    DbName = kz_util:format_account_id(DescendantId, 'encoded'),
    case kz_datamgr:get_result_ids(DbName, <<"periodic_fees/crossbar_listing">>, []) of
        {'ok', Ids} ->
            kz_datamgr:del_docs(DbName, Ids);
        {'error', _R} ->
            lager:debug("unable to get periodic_fees docs of ~s: ~p", [DescendantId, _R])
    end,
    timer:sleep(100),
    remove_periodic_fees_from_db(DescendantsIds).

-spec fetch_transactions_docs(ne_binary()) -> kz_json:objects().
fetch_transactions_docs(AccountId) ->
    {{Year,Month,_}, _ } = calendar:gregorian_seconds_to_datetime(kz_time:current_tstamp()),
    fetch_transactions_docs(AccountId, Year, Month).

-spec fetch_transactions_docs(ne_binary(), kz_year(), kz_month()) -> kz_json:objects().
fetch_transactions_docs(AccountId, Year, Month) ->
    ViewOptions = [{'year', Year}
                  ,{'month', Month}
                  ,'include_docs'
                  ],
    case kazoo_modb:get_results(AccountId, <<"transactions/by_timestamp">>, ViewOptions) of
        {'error', _} -> [];
        {'ok', ViewRes} -> [kz_json:get_value(<<"doc">>, JObj) || JObj <- ViewRes]
    end.

-spec filter_transactions(ne_binary(), kz_json:objects()) -> kz_json:objects().
filter_transactions(TrType, TrDocs) ->
    [TrDoc || TrDoc <- TrDocs
            ,kz_json:get_value(<<"pvt_type">>, TrDoc) == TrType
            ,kz_json:get_value(<<"pvt_reason">>, TrDoc) /= <<"database_rollup">>
    ].

-spec summ_transactions(kz_json:objects()) -> integer().
summ_transactions(TrDocs) ->
    lists:foldl(fun(X, Acc) -> Acc + kz_json:get_integer_value(<<"pvt_amount">>, X, 0) end, 0, TrDocs).

set_bom_balance(Amount, AccountId) when is_integer(Amount) ->
    AccountMODb = kazoo_modb:get_modb(AccountId),
    EncodedMODb = kz_util:format_account_modb(AccountMODb, 'encoded'),
    kz_datamgr:del_doc(EncodedMODb, <<"monthly_rollup">>),
    timer:sleep(500),
    wht_util:rollup(AccountId, Amount),
    case kazoo_modb:open_doc(AccountId, <<"monthly_rollup">>) of
        {'ok', CurrDoc} ->
            Transaction = kz_transaction:from_json(CurrDoc),
            case kz_transaction:type(Transaction) of
                <<"debit">> -> -1 * kz_transaction:amount(Transaction);
                <<"credit">> -> kz_transaction:amount(Transaction)
            end;
        {'error', 'not_found'} -> 'not_found';
        _ -> 'error'
    end;
set_bom_balance(_Amount, _AccountId) ->
    'not_integer'.

account_type(AccountId) ->
    case onlb_sql:is_prepaid(AccountId) of
        'true' -> <<"PRE">>;
        'false' -> <<"POS">>
    end.
