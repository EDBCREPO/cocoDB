#pragma once

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

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { struct cmd_t {
    function_t<void,cmd_t,function_t<void,string_t>> callback;
    string_t cmd; string_t val; string_t exp;
    string_t fid; string_t qid; string_t kid;
};}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { bool blocked = false; }

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { sqlite_t get_sqlite_db( string_t path ) {
    auto db = sqlite_t( path ); try { db.exec(
    R"( CREATE TABLE IF NOT EXISTS BUCKET (
        KID TEXT   NOT NULL, RID TEXT NOT NULL,
        EXP BIGNUM NOT NULL, VAL TEXT NOT NULL,
        NOW BIGNUM NOT NULL
    );)" ); } catch(...) {} return db;
}}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { GENERATOR( _task_ ){ 
protected:

    struct NODE {
        bool     blocked;
        string_t borrow ;
    };  ptr_t<NODE> obj ;

public: _task_() : obj( new NODE() ) {}

    coEmit( queue_t<cmd_t> list, socket_t cli ){
        if( cocoDB::blocked ){ return -1; }
    gnStart

        while( !list.empty() ){ do { 
            
            auto cmd  = list.first()->data; obj->blocked = true;
            auto self = type::bind( this ); obj->borrow.clear();
            
            cmd.callback.emit( cmd, [=]( string_t data ){
                list.erase( list.first() ); 
                self->obj->blocked = false;
                self->obj->borrow += data;
            });

            cmd.callback.free();

        } while(0); coWait( obj->blocked==true ); } 
        
        cli.write( obj->borrow );

    gnStop
    }

};}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { string_t get_item_kid( string_t key ){
    auto   hash = crypto::hash::SHA1(); hash.update(key);
    return hash.get();
}}

namespace cocoDB { string_t get_item_qid( cmd_t* item ){
    auto hash= crypto::hash::SHA1(); 
    hash.update( string::to_string( process::now()) );
    hash.update( encoder::key::generate(32) );
    hash.update( string::to_string( item ) );
    return hash.get();
}}

namespace cocoDB { ulong get_exp_val( string_t& val ){
    auto   EXP= string::to_ulong( val );
    auto   NOW= date  ::now();
    return EXP==0?0:(EXP+NOW);
}}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB {

    void cmd_flushall( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto dir = process::env::get("STORAGE_PATH");
        auto args= match.splice( 0,1 ); cmd_t cmd;
        if ( args.size() != 1 ){ throw ""; }

        blocked = true;

        for( auto x: fs::read_folder( dir ) ){
             auto y= path::join( dir, x );
             fs::writable( y );
        }

        blocked = false;
    }

    void cmd_next( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0, 1 );
        if ( args.size() != 1 )              { throw ""; }
        if ( regex::test( args[0], "quit" ) ){ throw ""; }
    }

    void cmd_match( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "MATCH";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto blk = queue_t<string_t>();

            auto sql = get_sqlite_db( dir ); 
            
            sql.onRelease.once([=](){
                if( blk.empty() ){ cb.emit("$-1\r\n");  return; }
                auto data = regex::format( "*${0}\r\n", blk.size() );
                auto n=blk.first(); while( n!=nullptr ){
                    data += n->data; n=n->next;
                }   cb.emit( data ); 
            });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}'"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ try {
                auto val = encoder::base64::btoa( item["VAL"] );
                if( !regex::test( val, cmd.val ) ){ return; }
                blk.push( regex::format( "$${0}\r\n${1}\r\n", val.size(), val ));
            } catch(...) {} });

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_match_count( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.cmd = "MCOUNT";
        cmd.val = args[2] ;

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto idx = type::bind( new ulong(0) );
            auto sql = get_sqlite_db( dir );

            sql.onRelease.once([=](){ cb.emit( regex::format(":${0}\r\n",*idx) ); });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ try {
                auto value = encoder::base64::btoa( item["VAL"] );
                if( !regex::test( value, cmd.val ) ){ return; }
            *idx += 1; } catch(...) {} });

        } catch(...) { cb.emit( ":0\r\n" );  } });

        list.push( cmd );

    }

    void cmd_match_range( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,5 ); cmd_t cmd;
        if ( args.size() != 5 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.val = args[2] ;
        cmd.cmd = "MRANGE";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto lim = type::bind( string::to_int( args[4] ) );
            auto off = type::bind( string::to_int( args[3] ) );
            auto blk = queue_t<string_t>();

            auto sql = get_sqlite_db( dir ); 
            
            sql.onRelease.once([=](){
                if( blk.empty() ){ cb.emit("$-1\r\n");  return; }
                auto data = regex::format( "*${0}\r\n", blk.size() );
                auto n=blk.first(); while( n!=nullptr ){
                    data += n->data; n=n->next;
                }   cb.emit( data ); 
            });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ try {
                auto val = encoder::base64::btoa( item["VAL"] );
                if( !regex::test( val, cmd.val ) ){ return; }
                if( *off != 0 ){ *off-=1; return;  }
                if( *lim == 0 ){ throw ""; }*lim-=1;
                blk.push( regex::format( "$${0}\r\n${1}\r\n", val.size(), val ));
            } catch(...) {} });

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_match_delete( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "MDEL" ;

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto sql = get_sqlite_db( dir ); 
            
            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

            sql.exec( regex::format(
                "SELECT VAL, RID FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ try {
                auto value = encoder::base64::btoa( item["VAL"] );
                if( !regex::test( value, cmd.val ) ){ return; }
            get_sqlite_db( dir ).async( regex::format(
                "DELETE FROM BUCKET WHERE RID='${0}'",
                item["RID"], date::now()
            )); } catch(...) {} });

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }


}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB {

    void cmd_shortex( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        auto n   = list.last();

        if ( args.size() != 2 )  { throw ""; }
        if ( n == nullptr )      { throw ""; }
        if ( n->data.cmd!="SET" ){ return; }

        n->data.exp = args[1];
    }

    void cmd_delete( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.cmd = "DEL";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); 
            
            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE KID='${0}'"
            , cmd.kid, date::now() ));

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_count( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.cmd = "COUNT";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }
            auto dne = type::bind( new bool(0) );

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                if( !(*dne) ){ cb.emit( ":0\r\n" );  }
            });

            sql.exec( regex::format(
              "SELECT COUNT(*) FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1})"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){ (*dne) = 1;
              cb.emit( regex::format( ":${0}\r\n", item["COUNT(*)"] ));
            });

        } catch(...) { cb.emit( ":0\r\n" );  } });

        list.push( cmd );

    }

    void cmd_get( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.cmd = "GET";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto blk = queue_t<string_t>();
            
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                if( blk.empty() ){ cb.emit("$-1\r\n"); return; }
                cb.emit( blk.first()->data ); 
            });

            sql.exec( regex::format(
              "SELECT VAL FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT 1"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){
              auto val = encoder::base64::btoa( item["VAL"] );
              blk.push( regex::format( "$${0}\r\n${1}\r\n", val.size(), val ));
            });

        } catch( ... ) { cb.emit( "$-1\r\n" ); } });

        list.push( cmd );

    }

    void cmd_set( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "SET";
        cmd.exp = "0";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto sql = get_sqlite_db( dir ); auto item=sql.exec( regex::format(
              "SELECT RID FROM BUCKET WHERE KID='${0}' LIMIT 1"
            , cmd.kid, date::now() ));

            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

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

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_ttl( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.cmd = "TTL";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }
            auto dne = type::bind( new bool(0) );

            auto sql = get_sqlite_db( dir ); sql.onRelease.once([=](){
                if( !(*dne) ){ cb.emit( ":-1\r\n" );  }
            });

            sql.exec( regex::format(
                "SELECT * FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT 1 OFFSET 0"
            , cmd.kid, date::now() ), [=]( sql_item_t item ){

                auto EXP = string::to_ulong( item["EXP"] );
                auto NOW = date  ::now(); *dne=1;

                if( EXP == 0 || NOW >= EXP )
                  { cb.emit( ":-1\r\n" );  return; }

                cb.emit( regex::format( ":${0}\r\n", EXP-NOW )); 
                

            });

        } catch(...) { cb.emit( ":-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_expire( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "EXP";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); 
            
            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

            sql.async( regex::format(
                "UPDATE BUCKET SET EXP=${0} WHERE KID='${1}' AND (EXP=0 OR EXP>${2})"
            ,   get_exp_val( cmd.val ), cmd.kid, date::now() ));

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_increase( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "INC";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto idx = type::bind( new llong( string::to_llong( cmd.val ) ) );

            auto sql = get_sqlite_db(dir); auto item=sql.exec( regex::format(
                "SELECT VAL,RID FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT 1"
            , cmd.kid, date::now() ));

            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

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

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_decrease( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "DEC";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            auto idx = type::bind( new llong( string::to_llong(cmd.val) ) );

            auto sql = get_sqlite_db( dir ); auto item=sql.exec( regex::format(
                "SELECT VAL,RID FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT 1"
            , cmd.kid, date::now() ));

            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

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

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_trim( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,4 ); cmd_t cmd;
        if ( args.size() != 4 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.cmd = "TRIM";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

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

            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT ${2} OFFSET ${3}"
            , cmd.kid, date::now(), slc[2], slc[0] ) );

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_range( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,4 ); cmd_t cmd;
        if ( args.size() != 4 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.cmd = "RANGE";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

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
                if( blk.empty() ){ cb.emit("$-1\r\n");  return; }
                auto data = regex::format( "*${0}\r\n", blk.size() );
                auto n=blk.first(); while( n!=nullptr ){
                    data += n->data; n=n->next;
                }   cb.emit( data ); 
            });

            sql.exec( regex::format(
                "SELECT VAL FROM BUCKET WHERE KID='${0}' AND (EXP=0 OR EXP>${1}) LIMIT ${2} OFFSET ${3}"
            , cmd.kid, date::now(), slc[2], slc[0] ), [=]( sql_item_t item ){
                auto val = encoder::base64::btoa( item["VAL"] );
                blk.push( regex::format( "$${0}\r\n${1}\r\n", val.size(), val ));
            });

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_push( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,3 ); cmd_t cmd;
        if ( args.size() != 3 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "PUSH";
        cmd.exp = "0";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );

            auto sql = get_sqlite_db( dir ); 
            
            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

            sql.async( regex::format( R"(
                INSERT INTO BUCKET ( NOW, RID, KID, EXP, VAL )
                VALUES  ( ${0}, '${1}', '${2}', ${3}, '${4}' );
            )", date::now(), cmd.qid,
                cmd.kid,  get_exp_val( cmd.exp ),
                encoder::base64::atob( cmd.val )
            ));

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_pop( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "POP";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir );  auto val = sql.exec( regex::format(
                "SELECT COUNT(*) FROM BUCKET WHERE KID='${0}'", cmd.kid
            )); 
            
            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

            if( val.empty() ){ throw ""; } if( val[0]["COUNT(*)"] == "0" ){ throw ""; }

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE KID='${0}' WHERE (EXP=0 OR EXP>${2}) LIMIT 1 OFFSET ${1}"
            , cmd.kid, string::to_ulong( val[0]["COUNT(*)"] )-1, date::now() ));

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_shift( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw ""; }

        cmd.fid = get_item_fid(args[1]);
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.val = args[2];
        cmd.cmd = "SHIFT";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); 
            
            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE KID='${0}' WHERE (EXP=0 OR EXP>${1}) LIMIT 1"
            , cmd.kid, date::now() ));

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );

    }

    void cmd_flush( array_t<string_t>& match, queue_t<cmd_t> list ){
        auto args= match.splice( 0,2 ); cmd_t cmd;
        if ( args.size() != 2 ){ throw ""; }
        
        cmd.kid = get_item_kid(args[1]);
        cmd.qid = get_item_qid( & cmd );
        cmd.fid = args[1];
        cmd.cmd = "FLUSH";

        cmd.callback = ([=]( cmd_t cmd, function_t<void,string_t> cb ){ try {

            auto dir = path::join( process::env::get("STORAGE_PATH"), cmd.fid );
            if( !fs::exists_file(dir) ){ throw ""; }

            auto sql = get_sqlite_db( dir ); 
            
            sql.onRelease.once([=](){ cb.emit( "+OK\r\n" ); });

            sql.async( regex::format(
                "DELETE FROM BUCKET WHERE (EXP<>0 AND EXP<${0})"
            , date::now() ));

        } catch(...) { cb.emit( "$-1\r\n" );  } });

        list.push( cmd );
        
    }

}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { void run_v1_process() {
    
    process::add( coroutine::add( COROUTINE(){
    coBegin ; coDelay( TIME_MINUTES( 30 ) );

        do{ for( auto x: fs::read_file( process::env::get("STORAGE_PATH") ) ){

            auto dir=path::join( process::env::get("STORAGE_PATH"),x );
            if( !fs::exists_file( dir ) ){ continue; }

            get_sqlite_db(dir).async( regex::format(
                "DELETE FROM BUCKET WHERE (EXP<>0 AND EXP<${0})"
            , date::now() ));

        } } while(0);
    
    coGoto(0) ; coFinish
    })); 

}}