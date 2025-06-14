#pragma once

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB {

    event_t<string_t> onSignalEvent;

    struct cmd_t {
        function_t<void,socket_t,cmd_t> callback;
        string_t cmd; string_t val; string_t exp;
        string_t fid; string_t qid; string_t kid;
    };

}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { string_t get_item_fid( string_t key ){
    auto name = regex::format( "${0}_COCOS_BUCKET", encoder::hash::get(key) );
    auto hash = crypto::hash::SHA1(); hash.update( name ); return hash.get();
}}

namespace cocoDB { ptr_t<ulong> get_slice_range( long x, long y, ulong size ){

    if( size == 0 || x == y ){ return nullptr; } 
    
    if( y > 0 ){ y--;        }
    if( x < 0 ){ x = size+x; } if( (ulong)x > size ){ return nullptr; }
    if( y < 0 ){ y = size+y; } if( (ulong)y > size ){ y = size;       }
    if( y < x ){ return nullptr; }

    ulong a = clamp( (ulong)y, 0UL, size );
    ulong b = clamp( (ulong)x, 0UL, a    );
    ulong c = a - b + 1;

    return ptr_t<ulong>({ b, a, c });

}}

namespace cocoDB { ptr_t<ulong> get_slice_trim( long x, long y, ulong size ){

    if( size == 0 || x == y ){ return nullptr; } 
    
    if( y > 0 ){ y--;        }
    if( x < 0 ){ x = size+x; } if( (ulong)x > size ){ return nullptr; }
    if( y < 0 ){ y = size+y; } if( (ulong)y > size ){ return nullptr; }
    if( y < x ){ return nullptr; }

    ulong a = clamp( (ulong)y, 0UL, size );
    ulong b = clamp( (ulong)x, 0UL, a    );
    ulong c = a - b + 1;

    return ptr_t<ulong>({ b, a, c });

}}

namespace cocoDB { string_t get_item_kid( string_t key ){
    auto   hash = crypto::hash::SHA1(); hash.update(key);
    return hash.get();
}}

namespace cocoDB { string_t get_item_qid( cmd_t& item ){
    auto mem = string_t( sizeof(cmd_t),'\0' );
    memcpy( mem.get(), &item, sizeof(cmd_t) );
    auto hash= crypto::hash::SHA1(); hash.update(mem);
    hash.update( string::to_string( process::now()) );
    hash.update( encoder::key::generate(32) );
    return hash.get();
}}

namespace cocoDB { ulong get_exp_val( string_t& val ){
    auto   EXP= string::to_ulong( val );
    auto   NOW= date  ::now();
    return EXP==0?0:(EXP+NOW);
}}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { sqlite_t get_sqlite_db( string_t path ) {
    sqlite_t db( path ); try { db.exec(
    R"( CREATE TABLE IF NOT EXISTS BUCKET (
        KID TEXT   NOT NULL, RID TEXT NOT NULL,
        EXP BIGNUM NOT NULL, VAL TEXT NOT NULL,
        NOW BIGNUM NOT NULL
    );)" ); } catch(...) {} return db;
}}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB {

    template< class T >
    void cmd_wipe( array_t<string_t>& match, T& cli ){
        auto args= match.splice( 0,1 ); cmd_t cmd;
        if ( args.size() != 1 ){ throw except_t(args[0]); }

        apify::add( *ws_client ).emit( "WIPE", "/api/v1/db", "WIPE" );
        cli.write( "+OK\r\n" );

    }

    template< class T >
    void cmd_match( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "MATCH";

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto blk = queue_t<string_t>();

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                auto data = regex::format( "*${0}\r\n", blk.size() );
                auto n=blk.first(); while( n!=nullptr ){ 
                    data += n->data; n=n->next; 
                }   cli.write( data );
            });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}'"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ try {
                auto val = encoder::base64::btoa( item["VAL"] );
                if( !regex::test( val, cmd.val ) ){ return; }
                blk.push( regex::format( "$${0}\r\n${1}\r\n", val.size(), val ));
            } catch(...) {} });

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_match_count( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.cmd = "MCOUNT";
        cmd.val = args[2] ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto idx = type::bind( new ulong(0) );
            auto sql = get_sqlite_db( dir );

            sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( regex::format(":${0}\r\n",*idx) );
            });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ try {
                auto value = encoder::base64::btoa( item["VAL"] );
                if( !regex::test( value, cmd.val ) ){ return; }
            *idx += 1; } catch(...) {} });

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_match_range( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,5 ); cmd_t cmd;
        if ( args.size() != 5 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2] ;
        cmd.cmd = "MRANGE";

        cmd.callback = ([=]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto lim = type::bind( string::to_int( args[4] ) );
            auto off = type::bind( string::to_int( args[3] ) );
            auto blk = queue_t<string_t>();

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) )); sql.free();
                auto data = regex::format( "*${0}\r\n", blk.size() );
                auto n=blk.first(); while( n!=nullptr ){ 
                    data += n->data; n=n->next; 
                }   cli.write( data );
            });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ try {
                auto val = encoder::base64::btoa( item["VAL"] );
                if( !regex::test( val, cmd.val ) ){ return; }
                if( *off != 0 ){ *off-=1; return;  }
                if( *lim == 0 ){ throw ""; }*lim-=1;
                blk.push( regex::format( "$${0}\r\n${1}\r\n", val.size(), val ));
            } catch(...) { sql.close(); } });

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_match_delete( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "MDEL" ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            sql.exec( regex::format(
                "SELECT VAL, RID FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ try {
                auto value = encoder::base64::btoa( item["VAL"] );
                if( !regex::test( value, cmd.val ) ){ return; }
            get_sqlite_db( dir ).async( regex::format(
                "DELETE FROM BUCKET WHERE RID='${0}'",
                item["RID"], date::now()
            )); } catch(...) {} });

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }


}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB {

    template< class T >
    void cmd_shortex( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        auto n   = self->list.last();

        if ( args.size() != 2 )  { throw except_t(args[0]); }
        if ( n == nullptr )      { throw except_t(args[0]); }
        if ( n->data.cmd!="SET" ){ return; }

        n->data.exp = args[1];

    }

    template< class T >
    void cmd_delete( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.cmd = "DEL"  ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE KID='${0}'"
            , cmd.kid, date::now() ));

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_count( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.cmd = "COUNT";

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }
            auto dne = type::bind( new bool(0) );

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                if( !(*dne) ){ cli.write( "$-1\r\n" ); }
            });

            sql.exec( regex::format(
                "SELECT COUNT(*) FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ (*dne) = 1;
              cli.write( regex::format( ":${0}\r\n", item["COUNT(*)"] ));
            });

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_get( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.cmd = "GET"  ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }
            auto blk = queue_t<string_t>(); 

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                auto data = regex::format( "*${0}\r\n", blk.size() );
                auto n=blk.first(); while( n!=nullptr ){ 
                    data += n->data; n=n->next; 
                }   cli.write( data );
            });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT 1"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ 
                auto val = encoder::base64::btoa( item["VAL"] );
                blk.push( regex::format( "$${0}\r\n${1}\r\n", val.size(), val ));
            });

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_set( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "SET"  ;
        cmd.exp = "0"    ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto sql = get_sqlite_db( dir ); auto item=sql.exec( regex::format(
                "SELECT RID FROM BUCKET WHERE KID='${0}' LIMIT 1"
            , cmd.kid, date::now() )); 
            
            sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            if( !item.empty() ){

                sql.exec( regex::format( R"(
                    UPDATE BUCKET SET NOW=${0}, EXP=${1}, VAL='${2}' WHERE RID='${3}' LIMIT 1
                )", date::now(), get_exp_val(cmd.exp), encoder::base64::atob(cmd.val), item[0]["RID"] ));

            } else {

                sql.exec( regex::format( R"(
                    INSERT INTO BUCKET ( NOW, RID, KID, EXP, VAL )
                    VALUES  ( ${0}, '${1}', '${2}', ${3}, '${4}' );
                )", date::now(), cmd.qid,
                    cmd.kid, get_exp_val( cmd.exp ),
                    encoder::base64::atob(cmd.val)
                ));

            }

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_ttl( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.cmd = "TTL"  ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }
            auto dne = type::bind( new bool(0) );

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                if( !(*dne) ){ cli.write( "$-1\r\n" ); }
            });

            sql.exec( regex::format(
                "SELECT * FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT 1 OFFSET 0"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){

                auto EXP = string::to_ulong( item["EXP"] );
                auto NOW = date  ::now(); *dne=1;

                if( EXP == 0 || NOW >= EXP )
                  { cli.write( ":-1\r\n" ); return; }

                cli.write( regex::format( ":${0}\r\n", EXP-NOW ));

            });

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_expire( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "EXP"  ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            sql.async( regex::format(
                "UPDATE BUCKET SET EXP=${0} WHERE KID='${1}' AND (EXP=0 OR EXP>${2})"
            ,   get_exp_val( cmd.val ), cmd.kid, date::now() ));

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_increase( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "INC"  ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto idx = type::bind( new llong( string::to_llong( cmd.val ) ) );

            auto sql = get_sqlite_db(dir); auto item=sql.exec( regex::format(
                "SELECT VAL,RID FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT 1"
            , cmd.kid, date::now() ));

            sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            if( !item.empty() ) {

                *idx = string::to_llong( encoder::base64::btoa(item[0]["VAL"]) ) + *idx;
                sql.async( regex::format(
                    "UPDATE BUCKET SET VAL='${1}' WHERE RID='${0}' LIMIT 1"
                , item[0]["RID"], encoder::base64::atob( string::to_string(*idx) )
                ));

            } else {

                *idx = 0 + *idx; sql.async( regex::format( R"(
                    INSERT INTO BUCKET ( NOW, KID, EXP, RID, VAL )
                    VALUES  ( ${0}, '${1}', ${2}, '${3}', '${4}' );
                )", date::now(),cmd.kid ,
                    get_exp_val(cmd.exp), cmd.qid,
                    encoder::base64::atob( string::to_string( *idx ) )
                ));
            }

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_decrease( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "DEC"  ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto idx = type::bind( new llong( string::to_llong(cmd.val) ) );

            auto sql = get_sqlite_db( dir ); auto item=sql.exec( regex::format(
                "SELECT VAL,RID FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT 1"
            , cmd.kid, date::now() ));
            
            sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            if( !item.empty() ) {

                *idx = string::to_llong( encoder::base64::btoa(item[0]["VAL"]) ) - *idx;
                sql.async( regex::format(
                    "UPDATE BUCKET SET VAL='${1}' WHERE RID='${0}' LIMIT 1"
                , item[0]["RID"], encoder::base64::atob( string::to_string(*idx) )
                ));

            } else {

                *idx = 0 - *idx; sql.async( regex::format( R"(
                    INSERT INTO BUCKET ( NOW, KID, EXP, RID, VAL )
                    VALUES  ( ${0}, '${1}', ${2}, '${3}', '${4}' );
                )", date::now(),cmd.kid ,
                    get_exp_val(cmd.exp), cmd.qid,
                    encoder::base64::atob( string::to_string( *idx ) )
                ));
            }

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_trim( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,4 ); cmd_t cmd;
        if ( args.size() != 4 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.cmd = "TRIM" ;

        cmd.callback = ([=]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); auto raw = sql.exec( regex::format(
                "SELECT COUNT(*) FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() )); if( raw.empty() ){ throw ""; }

            auto slc = get_slice_trim(
                string::to_int  (args[2]),
                string::to_int  (args[3]),
                string::to_ulong(raw [0]["COUNT(*)"])
            );  if( slc.empty() ){ throw ""; }

            sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT ${2} OFFSET ${3}"
            , cmd.kid, date::now(), slc[2], slc[0] ) );

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_range( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,4 ); cmd_t cmd;
        if ( args.size() != 4 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.cmd = "RANGE";

        cmd.callback = ([=]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto blk = queue_t<string_t>(); auto sql = get_sqlite_db( dir ); 
            auto raw = sql.exec( regex::format(
                "SELECT COUNT(*) FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() )); if( raw.empty() ){ throw ""; }

            auto slc = get_slice_range(
                string::to_int  (args[2]),
                string::to_int  (args[3]),
                string::to_ulong(raw [0]["COUNT(*)"])
            );  if( slc.empty() ){ throw ""; }

            sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                auto data = regex::format( "*${0}\r\n", blk.size() );
                auto n=blk.first(); while( n!=nullptr ){ 
                    data += n->data; n=n->next; 
                }   cli.write( data );
            });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT ${2} OFFSET ${3}"
            , cmd.kid, date::now(), slc[2], slc[0] ), [=]( sql_item_t item ){
                auto val = encoder::base64::btoa( item["VAL"] );
                blk.push( regex::format( "$${0}\r\n${1}\r\n", val.size(), val ));
            });

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_push( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "PUSH" ;
        cmd.exp = "0"    ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            sql.async( regex::format( R"(
                INSERT INTO BUCKET ( NOW, RID, KID, EXP, VAL )
                VALUES  ( ${0}, '${1}', '${2}', ${3}, '${4}' );
            )", date::now(), cmd.qid,
                cmd.kid,  get_exp_val( cmd.exp ),
                encoder::base64::atob( cmd.val )
            ));

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_pop( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "POP"  ;

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir );  auto val = sql.exec( regex::format(
                "SELECT COUNT(*) FROM BUCKET WHERE KID='${0}'", cmd.kid
            )); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            if( val.empty() ){ throw ""; } if( val[0]["COUNT(*)"] == "0" ){ throw ""; }

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE KID='${0}' WHERE (EXP=0 OR EXP>${2}) LIMIT 1 OFFSET ${1}"
            , cmd.kid, string::to_ulong( val[0]["COUNT(*)"] )-1, date::now() ));

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );
    }

    template< class T >
    void cmd_shift( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw except_t(args[0]); }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2];
        cmd.cmd = "SHIFT";

        cmd.callback = ([]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE KID='${0}' WHERE (EXP=0 OR EXP>${1}) LIMIT 1"
            , cmd.kid, date::now() ));

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

    template< class T >
    void cmd_flush( array_t<string_t>& match, T& self ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw except_t(args[0]); }

        cmd.kid = get_item_kid(args[1]);
        cmd.fid = args[1];
        cmd.cmd = "FLUSH";

        cmd.callback = ([=]( socket_t cli, cmd_t cmd ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
                cli.write( "+OK\r\n" );
            });

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE (EXP<>0 AND EXP<${0})"
            , date::now() ));

        } catch(...) {
              apify::add( *ws_client ).emit( "UNLOCK", "/api/v1/db", json::stringify(
              object_t({ { "fid", cmd.fid },{ "qid", cmd.qid } }) ));
              cli.write( "$-1\r\n" );
        } });

        cmd.qid = get_item_qid( cmd );
        self->list.push( cmd );

    }

}

/*────────────────────────────────────────────────────────────────────────────*/
