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
-export([compare_balances/2
        ,compare_credits/2
        ,compare_usage/2
        ,sync_bom_balance/2
        ,sync_customer_data/2
        ,import_accounts/3
        ,is_allowed/1
        ]).

%% Dev temp
-export([fetch_view_docs/2
        ,filter_transactions/2
        ,summ_transactions/1
        ]).

-include_lib("tasks/src/tasks.hrl").
-include_lib("kazoo_services/include/kz_service.hrl").
-include("onlb.hrl").

-define(CATEGORY, "onlb").

-define(TRANS_VIEW, <<"transactions/by_timestamp">>).
-define(LEDGERS_VIEW, <<"ledgers/listing_by_service">>).

-define(ACTIONS, [<<"compare_balances">>
                 ,<<"compare_credits">>
                 ,<<"compare_usage">>
                 ,<<"sync_bom_balance">>
                 ,<<"sync_customer_data">>
                 ,<<"import_accounts">>
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
output_header(<<"compare_balances">>) ->
    [<<"account_id">>
    ,<<"account_name">>
    ,<<"bom_kazoo_balance">>
    ,<<"bom_lb_balance">>
    ,<<"bom_difference">>
    ,<<"current_kazoo_balance">>
    ,<<"current_lb_balance">>
    ,<<"current_difference">>
    ];

output_header(<<"compare_credits">>) ->
    [<<"account_id">>
    ,<<"account_name">>
    ,<<"kazoo_credit">>
    ,<<"lb_credit">>
    ,<<"difference">>
    ];

output_header(<<"compare_usage">>) ->
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
    ];

output_header(<<"sync_customer_data">>) ->
    [<<"account_id">>
    ,<<"account_name">>
    ,<<"result">>
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
action(<<"compare_balances">>) ->
    [{<<"description">>, <<"compare Kazoo and LanBilling balances">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"compare_credits">>) ->
    [{<<"description">>, <<"compare Kazoo and LanBilling current month total credit">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"compare_usage">>) ->
    [{<<"description">>, <<"compare Kazoo and LanBilling current month charges">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"sync_bom_balance">>) ->
    [{<<"description">>, <<"sync Kazoo bom balance to LanBilling's one">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"sync_customer_data">>) ->
    [{<<"description">>, <<"Extract LanBilling's customer data">>}
    ,{<<"doc">>, <<"Just an experimentsl feature.">>}
    ];

action(<<"import_accounts">>) ->
    Mandatory = ?IMPORT_ACCOUNTS_MANDATORY_FIELDS,
    Optional = ?IMPORT_ACCOUNTS_DOC_FIELDS -- Mandatory,

    [{<<"description">>, <<"Bulk-create accounts using account_names list">>}
    ,{<<"doc">>, <<"Creates accounts from file">>}
    ,{<<"expected_content">>, <<"text/csv">>}
    ,{<<"mandatory">>, Mandatory}
    ,{<<"optional">>, Optional}
    ].

%%% Verifiers


%%% Appliers

-spec compare_balances(kz_tasks:extra_args(), kz_tasks:iterator()) -> kz_tasks:iterator().
compare_balances(#{account_id := AccountId}, init) ->
    {'ok', get_children(AccountId)};
compare_balances(_, []) -> stop;
compare_balances(_, [SubAccountId | DescendantsIds]) ->
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

-spec compare_credits(kz_tasks:extra_args(), kz_tasks:iterator()) -> kz_tasks:iterator().
compare_credits(#{account_id := AccountId}, init) ->
    {'ok', get_children(AccountId)};
compare_credits(_, []) -> stop;
compare_credits(_, [SubAccountId | DescendantsIds]) ->
    {'ok', JObj} = kz_account:fetch(SubAccountId),
    KazooCredit =
        wht_util:units_to_dollars(summ_transactions(filter_transactions({<<"pvt_type">>,<<"credit">>}
                                                                       ,fetch_view_docs(SubAccountId, ?TRANS_VIEW)
                                                                       )
                                                   )
                                 ),
    LbCredit = onlb_sql:curr_month_credit(SubAccountId),
    Difference =
        try kz_term:to_float(KazooCredit) - kz_term:to_float(LbCredit) catch _:_ -> 'math_error' end,
    {[SubAccountId
     ,kz_account:name(JObj)
     ,try onbill_util:price_round(KazooCredit) catch _:_ -> KazooCredit end
     ,try onbill_util:price_round(LbCredit) catch _:_ -> LbCredit end
     ,try onbill_util:price_round(Difference) catch _:_ -> Difference end
     ], DescendantsIds}.

-spec compare_usage(kz_tasks:extra_args(), kz_tasks:iterator()) -> kz_tasks:iterator().
compare_usage(#{account_id := AccountId}, init) ->
    {'ok', get_children(AccountId)};
compare_usage(_, []) -> stop;
compare_usage(_, [SubAccountId | DescendantsIds]) ->
    {'ok', JObj} = kz_account:fetch(SubAccountId),
    {{Y,M,_}, _ } = calendar:gregorian_seconds_to_datetime(kz_time:current_tstamp()),
    {PY, PM} = onbill_util:prev_month(Y, M),
    PrevMonthKazooUsage =
        wht_util:units_to_dollars(summ_transactions(filter_transactions({<<"pvt_type">>,<<"debit">>}
                                                                       ,fetch_view_docs(SubAccountId, ?TRANS_VIEW, PY, PM)
                                                                       )
                                                   )
                                 ),
    PrevMonthKazooLedgers =
        wht_util:units_to_dollars(summ_transactions(filter_transactions({<<"pvt_ledger_type">>,<<"debit">>}
                                                                       ,fetch_view_docs(SubAccountId, ?LEDGERS_VIEW, PY, PM)
                                                                       )
                                                   )
                                 ),
    PrevMonthLbUsage = onlb_sql:calc_prev_month_exp(SubAccountId),
    PrevMonthDifference =
        try
          kz_term:to_float(PrevMonthKazooUsage) + kz_term:to_float(PrevMonthKazooLedgers) - kz_term:to_float(PrevMonthLbUsage)
        catch
           _:_ -> 'math_error'
        end,
    KazooUsage =
        wht_util:units_to_dollars(summ_transactions(filter_transactions({<<"pvt_type">>,<<"debit">>}
                                                                       ,fetch_view_docs(SubAccountId, ?TRANS_VIEW)
                                                                       )
                                                   )
                                 ),
    KazooLedgers =
        wht_util:units_to_dollars(summ_transactions(filter_transactions({<<"pvt_ledger_type">>,<<"debit">>}
                                                                       ,fetch_view_docs(SubAccountId, ?LEDGERS_VIEW)
                                                                       )
                                                   )
                                 ),
    LbUsage = onlb_sql:calc_curr_month_exp(SubAccountId),
    Difference =
        try
          kz_term:to_float(KazooUsage) + kz_term:to_float(KazooLedgers) - kz_term:to_float(LbUsage)
        catch
           _:_ -> 'math_error'
        end,
    {[SubAccountId
     ,kz_account:name(JObj)
     ,try onbill_util:price_round(PrevMonthKazooUsage + PrevMonthKazooLedgers) catch _:_ -> PrevMonthKazooUsage end
     ,try onbill_util:price_round(PrevMonthLbUsage) catch _:_ -> PrevMonthLbUsage end
     ,try onbill_util:price_round(PrevMonthDifference) catch _:_ -> PrevMonthDifference end
     ,try onbill_util:price_round(KazooUsage + KazooLedgers) catch _:_ -> KazooUsage end
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

-spec sync_customer_data(kz_tasks:extra_args(), kz_tasks:iterator()) -> kz_tasks:iterator().
sync_customer_data(#{account_id := AccountId}, init) ->
    {'ok', get_children(AccountId)};
sync_customer_data(_, []) -> stop;
sync_customer_data(_, [SubAccountId | DescendantsIds]) ->
    {'ok', JObj} = kz_account:fetch(SubAccountId),
    case onlb_sql:lbuid_by_uuid(SubAccountId) of
        'undefined' ->
            {[SubAccountId ,kz_account:name(JObj) , 'no_such_account_in_lb'], DescendantsIds};
        _ -> 
            onlb:lb_to_kazoo_sync(SubAccountId),
            kz_services:reconcile(SubAccountId),
            {[SubAccountId ,kz_account:name(JObj) , 'processed'], DescendantsIds}
    end.

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

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec get_children(ne_binary()) -> ne_binaries().
get_children(AccountId) ->
    ViewOptions = [{'startkey', [AccountId]}
                  ,{'endkey', [AccountId, kz_json:new()]}
                  ],
    case kz_datamgr:get_results(?KZ_ACCOUNTS_DB, <<"accounts/listing_by_children">>, ViewOptions) of
        {'ok', JObjs} -> [kz_doc:id(JObj) || JObj <- JObjs];
        {'error', _R} ->
            lager:debug("unable to get children of ~s: ~p", [AccountId, _R]),
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

-spec fetch_view_docs(ne_binary(), ne_binary()) -> kz_json:objects().
fetch_view_docs(AccountId, View) ->
    {{Year,Month,_}, _} = calendar:gregorian_seconds_to_datetime(kz_time:current_tstamp()),
    fetch_view_docs(AccountId, View, Year, Month).

-spec fetch_view_docs(ne_binary(), ne_binary(), kz_year(), kz_month()) -> kz_json:objects().
fetch_view_docs(AccountId, View, Year, Month) ->
    ViewOptions = [{'year', Year}
                  ,{'month', Month}
                  ,'include_docs'
                  ],
    fetch_view_docs(AccountId, View, ViewOptions).

-spec fetch_view_docs(ne_binary(), ne_binary(), kz_proplist()) -> kz_json:objects().
fetch_view_docs(AccountId, View, ViewOptions) ->
    case kazoo_modb:get_results(AccountId, View, ViewOptions) of
        {'error', _} -> [];
        {'ok', ViewRes} -> [kz_json:get_value(<<"doc">>, JObj) || JObj <- ViewRes]
    end.

-spec filter_transactions(tuple(), kz_json:objects()) -> kz_json:objects().
filter_transactions({K, V}, TrDocs) ->
    [TrDoc || TrDoc <- TrDocs
            ,kz_json:get_value(K, TrDoc) == V
            ,kz_json:get_value(<<"pvt_reason">>, TrDoc) /= <<"database_rollup">>
    ].

-spec summ_transactions(kz_json:objects()) -> integer().
summ_transactions(TrDocs) ->
    lists:foldl(fun(X, Acc) -> Acc + kz_json:get_first_defined([<<"pvt_amount">>,<<"amount">>], X, 0) end, 0, TrDocs).

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
