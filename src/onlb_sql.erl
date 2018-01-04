-module(onlb_sql).
-author("Kirill Sysoev <kirill.sysoev@gmail.com>").

-export([maybe_mysql_child/0
        ,lbuid_by_uuid/1
        ,account_balance/1
        ,get_main_agrm_id/1
        ,update_lb_account/3
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
    UUID = lbuid_by_uuid(AccountId),
    case mysql_poolboy:query(?LB_MYSQL_POOL
                            ,<<"SELECT agrm_id from agreements where uid  = ? and oper_id = 1 limit 1">>
                            ,[UUID])
    of
        {ok,_,[[AgrmId]]} -> AgrmId;
        _ -> 'undefined'
    end.

-spec update_lb_account(integer(), ne_binary(), kz_json:object()) -> any().
update_lb_account(UID, _AccountId, Doc) ->
    AccountName = kz_json:get_binary_value(<<"account_name">>, Doc, <<>>),
    Type =
        case kz_json:get_binary_value(<<"customer_type">>, Doc) of
            <<"personal">> -> 1;
            _ -> 2
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
lager:info("IAMC update_lb_account QueryString: ~p",[QueryString]),
 Res =   mysql_poolboy:query(?LB_MYSQL_POOL, QueryString, Values),
lager:info("IAMC update_lb_account mysql_poolboy:query Res: ~p",[Res]).
 %   mysql_poolboy:query(?LB_MYSQL_POOL, QueryString).
    
