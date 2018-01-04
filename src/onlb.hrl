-ifndef(ONLB_HRL).

-include_lib("kazoo_stdlib/include/kz_types.hrl").
-include_lib("kazoo_stdlib/include/kz_log.hrl").
-include_lib("kazoo_stdlib/include/kz_databases.hrl").
-include_lib("kazoo_documents/include/kazoo_documents.hrl").

-define(ONBILL_DOC, <<"onbill">>).
-define(ONBILL_DB(ResellerId), <<"onbill-", ResellerId/binary>>).

-define(ONLB_HRL, 'true').
-endif.
