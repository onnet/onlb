-module(onlb_sql).
-author("Kirill Sysoev <kirill.sysoev@gmail.com>").

-export([maybe_mysql_child/0
        ,lbuid_by_uuid/1
        ,account_balance/1
        ,get_main_agrm_id/1
        ,update_lb_account/3
        ,curr_month_credit/1
        ,bom_balance/1
        ,calc_curr_month_exp/1
        ,calc_prev_month_exp/1
        ,is_prepaid/1
        ,agreements_data/1
        ]).

-include_lib("onlb.hrl").

-define(LB_MYSQL_POOL, 'lb_mysql').

-spec maybe_mysql_child() -> kz_proplists().
maybe_mysql_child() ->
    case kapps_config:get_is_true(<<"onlb">>, <<"mysql_pool_enable">>, 'false') of
        'true' ->
            PoolOptions  = [{size, 10}, {max_overflow, 20}],
            MySqlOptions = [{host, kapps_config:get_string(<<"onlb">>, <<"mysql_host">>, <<"localhost">>)}
                           ,{user, kapps_config:get_string(<<"onlb">>, <<"mysql_user">>, <<"user">>)}
                           ,{password, kapps_config:get_string(<<"onlb">>, <<"mysql_password">>, <<"password">>)}
                           ,{database, kapps_config:get_string(<<"onlb">>, <<"mysql_database">>, <<"database">>)}
                           ],
            [mysql_poolboy:child_spec(?LB_MYSQL_POOL, PoolOptions, MySqlOptions)];
        'false' ->
            []
    end.

-spec lbuid_by_uuid(ne_binary()) -> any().
lbuid_by_uuid(AccountId) ->
    case mysql_poolboy:query(?LB_MYSQL_POOL
                            ,<<"select uid from accounts where uuid = ? limit 1">>
                            ,[AccountId])
    of
        {ok,_,[[Uid]]} -> Uid;
        _ -> 'undefined'
    end.

-spec account_balance(ne_binary()) -> any().
account_balance(AccountId) ->
    case lbuid_by_uuid(AccountId) of
        'undefined' -> 'undefined';
        UID ->
            case mysql_poolboy:query(?LB_MYSQL_POOL
                                    ,<<"SELECT COALESCE(sum(balance),0) FROM agreements  where uid = ? and agreements.archive = 0">>
                                    ,[UID])
            of
                {ok,_,[[Amount]]} -> Amount;
                _ -> 'undefined'
            end
    end.

-spec get_main_agrm_id(ne_binary()) -> any().
get_main_agrm_id(AccountId) ->
    case lbuid_by_uuid(AccountId) of
        'undefined' -> 'undefined';
        UID ->
            case mysql_poolboy:query(?LB_MYSQL_POOL
                                    ,<<"SELECT agrm_id from agreements where uid  = ? and oper_id = 1 limit 1">>
                                    ,[UID])
            of
                {ok,_,[[AgrmId]]} -> AgrmId;
                _ -> 'undefined'
            end
    end.

-spec update_lb_account(integer(), ne_binary(), kz_json:object()) -> any().
update_lb_account(UID, _AccountId, Doc) ->
    AccountName = kz_json:get_binary_value(<<"account_name">>, Doc, <<>>),
    Type =
        case kz_json:get_binary_value(<<"customer_type">>, Doc) of
            <<"personal">> -> 2;
            _ -> 1
        end,
    QueryString =
        <<"UPDATE `billing`.`accounts` SET name = ? "
         ,",type = ? "
         ,",inn = ? "
         ,",kpp = ? "
         ,",ogrn = ? "
         ,",bank_name = ? "
         ,",branch_bank_name = ? "
         ,",bik = ? "
         ,",settl = ? "
         ,",corr = ? "
         ," WHERE accounts.uid = ?">>,
    Values = [kz_term:to_binary(AccountName)
             ,kz_term:to_binary(Type)
             ,kz_json:get_binary_value(<<"account_inn">>, Doc, <<>>)
             ,kz_json:get_binary_value(<<"account_kpp">>, Doc, <<>>)
             ,kz_json:get_binary_value(<<"account_ogrn">>, Doc, <<>>)
             ,kz_json:get_binary_value([<<"banking_details">>,<<"bank_name">>], Doc, <<>>)
             ,kz_json:get_binary_value([<<"banking_details">>,<<"bank_branch_name">>], Doc, <<>>)
             ,kz_json:get_binary_value([<<"banking_details">>,<<"bik">>], Doc, <<>>)
             ,kz_json:get_binary_value([<<"banking_details">>,<<"settlement_account">>], Doc, <<>>)
             ,kz_json:get_binary_value([<<"banking_details">>,<<"correspondent_account">>], Doc, <<>>)
             ,kz_term:to_binary(UID)
             ],
    mysql_poolboy:query(?LB_MYSQL_POOL, QueryString, Values).
    
-spec curr_month_credit(ne_binary()) -> any().
curr_month_credit(AccountId) ->
    case get_main_agrm_id(AccountId) of
        'undefined' -> 'undefined';
        AgrmId ->
            case mysql_poolboy:query(?LB_MYSQL_POOL
                                    ,<<"SELECT SUM(amount) FROM payments where pay_date >= DATE_FORMAT(NOW() ,'%Y-%m-01') and agrm_id = ?">>
                                    ,[AgrmId]
                                    )
            of
                {ok,_,[['null']]} -> 0.0;
                {ok,_,[[Amount]]} -> Amount;
                _ -> 'undefined'
        end
    end.

-spec bom_balance(ne_binary()) -> any().
bom_balance(AccountId) ->
    QStr = <<"SELECT  COALESCE(sum(balances.balance),0) "
            ,"FROM agreements, accounts, balances "
            ,"where agreements.uid = accounts.uid "
            ,"and agreements.agrm_id = balances.agrm_id "
            ,"and accounts.uuid != '' "
            ,"and balances.balance != 0 "
            ,"and balances.date = DATE_FORMAT(NOW() ,'%Y-%m-01') "
            ,"and accounts.uuid = ?"
           >>,
    case mysql_poolboy:query(?LB_MYSQL_POOL, QStr, [AccountId]) of
        {ok,_,[[Uid]]} -> Uid;
        _ -> 'undefined'
    end.

-spec calc_curr_month_exp(ne_binary()) -> any().
calc_curr_month_exp(AccountId) ->
    case lbuid_by_uuid(AccountId) of
        'undefined' -> 'undefined';
        UID ->
            {{Year,Month,Day}, _ } = calendar:gregorian_seconds_to_datetime(kz_time:current_tstamp()),
            Today = io_lib:format("~w~2..0w~2..0w",[Year, Month, Day]),
            QueryString = io_lib:format("Select COALESCE(ifnull((SELECT sum(amount) FROM  tel001~s where uid = ~p),0) + ifnull((SELECT sum(amount) FROM  day where Month(timefrom) = Month(Now()) and Year(timefrom) = Year(Now()) and uid = ~p),0) + (Select sum(amount) from charges where agrm_id = (SELECT agrm_id FROM agreements where uid = ~p and oper_id = 1 and archive = 0) and Month(period) = Month(Now()) and Year(period) = Year(Now())),0)",[Today,UID,UID,UID]),
            QueryCheckTableString = io_lib:format("show tables like 'tel001~s'", [Today]),
            case mysql_poolboy:query(?LB_MYSQL_POOL, QueryCheckTableString) of
                {ok,_,[]} -> 'undefined';
                _  ->
                    case mysql_poolboy:query(?LB_MYSQL_POOL, QueryString) of
                        {ok,_,[[Amount]]} -> Amount;
                        _ -> 'undefined'
                    end
            end
    end.

-spec calc_prev_month_exp(ne_binary()) -> any().
calc_prev_month_exp(AccountId) ->
    case lbuid_by_uuid(AccountId) of
        'undefined' -> 'undefined';
        UID ->
            QueryString = io_lib:format("Select COALESCE(ifnull((SELECT sum(amount) FROM  day where Month(timefrom) = Month(DATE_ADD(Now(), INTERVAL -1 MONTH)) and Year(timefrom) = Year(DATE_ADD(Now(), INTERVAL -1 MONTH)) and uid = ~p),0) + (Select sum(amount) from charges where agrm_id = (SELECT agrm_id FROM agreements where uid = ~p and oper_id = 1 and archive = 0) and Month(period) = Month(DATE_ADD(Now(), INTERVAL -1 MONTH)) and Year(period) = Year(DATE_ADD(Now(), INTERVAL -1 MONTH))),0)",[UID,UID]),
            case mysql_poolboy:query(?LB_MYSQL_POOL, QueryString) of
                {ok,_,[[Amount]]} -> Amount;
                _ -> 'undefined'
            end
    end.

-spec is_prepaid(ne_binary()) -> boolean().
is_prepaid(AccountId) ->
    case lbuid_by_uuid(AccountId) of
        'undefined' -> 'true';
        UID ->
            QueryString = <<"SELECT 1 FROM tarifs, vgroups where tarifs.tar_id = vgroups.tar_id and vgroups.uid = ?  and tarifs.act_block = 2 limit 1">>,
            case mysql_poolboy:query(?LB_MYSQL_POOL, QueryString, [UID]) of
                {ok,_,[]} -> 'false';
                _ -> 'true'
            end
    end.

-spec agreements_data(ne_binary()) -> kz_proplists().
agreements_data(AccountId) ->
    case lbuid_by_uuid(AccountId) of
        'undefined' -> 'undefined';
        UID ->
            QueryString = <<"select oper_id,number,date from agreements where uid = ?">>,
            case mysql_poolboy:query(?LB_MYSQL_POOL, QueryString, [UID]) of
                {ok,_,Res} -> Res;
                _ -> []
            end
    end.
