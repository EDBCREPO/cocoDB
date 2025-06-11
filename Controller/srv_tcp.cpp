#pragma once
#define PATTERN "\"([^\"]+)\"|\'([^\']+)\'|([^\r \n\t\"\']+)"

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { GENERATOR( cmd_task ){
public: 
    
    ptr_t<uchar>  state=ptr_t<uchar>({ 1, 0 });
    queue_t<cmd_t> list;
    event_t<>   onDrain;

   /*.........................................................................*/

    void close() const noexcept { if( state[0]==0 ){ return; } state[0]=0; }

    /*.........................................................................*/

    void append( socket_t cli, string_t cmd ) const noexcept { try {

        auto match = regex::get_memory( cmd, PATTERN );
        auto self  = type ::bind( this );

          if( match.empty() ) { throw except_t(); } while( !match.empty() ) {
          if( regex::test(match[0],"quit"    ,true) ){                      throw except_t(); }
        elif( regex::test(match[0],"flushall",true) ){ cmd_wipe(match,cli);         continue; }
        elif( regex::test(match[0],"mcount"  ,true) ){ cmd_match_count(match,self); continue; }
        elif( regex::test(match[0],"mrange"  ,true) ){ cmd_match_range(match,self); continue; }
        elif( regex::test(match[0],"match"   ,true) ){ cmd_match(match,self);       continue; }
        elif( regex::test(match[0],"flush"   ,true) ){ cmd_flush(match,self);       continue; }
        elif( regex::test(match[0],"mdel"    ,true) ){ cmd_match_delete(match,self);continue; }
        elif( regex::test(match[0],"del"     ,true) ){ cmd_delete(match,self);      continue; }
        elif( regex::test(match[0],"get"     ,true) ){ cmd_get(match,self);         continue; }
        elif( regex::test(match[0],"set"     ,true) ){ cmd_set(match,self);         continue; }
        elif( regex::test(match[0],"ttl"     ,true) ){ cmd_ttl(match,self);         continue; }
        elif( regex::test(match[0],"exp"     ,true) ){ cmd_expire(match,self);      continue; }
        elif( regex::test(match[0],"ex"      ,true) ){ cmd_shortex(match,self);     continue; }
        elif( regex::test(match[0],"inc"     ,true) ){ cmd_increase(match,self);    continue; }
        elif( regex::test(match[0],"dec"     ,true) ){ cmd_decrease(match,self);    continue; }
        elif( regex::test(match[0],"pop"     ,true) ){ cmd_pop(match,self);         continue; }
        elif( regex::test(match[0],"shift"   ,true) ){ cmd_shift(match,self);       continue; }
        elif( regex::test(match[0],"push"    ,true) ){ cmd_push(match,self);        continue; }
        elif( regex::test(match[0],"trim"    ,true) ){ cmd_trim(match,self);        continue; }
        elif( regex::test(match[0],"count"   ,true) ){ cmd_count(match,self);       continue; }
        elif( regex::test(match[0],"range"   ,true) ){ cmd_range(match,self);       continue; } 
        throw except_t(match[0]); }

    } catch(...) { list.free(); cli.close(); }}

    /*.........................................................................*/

    void emit( socket_t cli, string_t qid ) const noexcept {
        if( state[0] == 0 || list.empty() ){ return; }
        if(  qid!= list.first()->data.qid ){ return; }        
        auto cmd = list.first()->data;

        list.first()->data.callback( cli, cmd );
        list.shift(); state[1]=0;
    }

    /*.........................................................................*/

    coEmit(){
        if( state[0]==0 && list.empty() ){ onDrain.emit(); return -1; }
    gnStart ; coWait( list.empty()==1 ); 

        apify::add( *ws_client ).emit( "LOCK", "/api/v1/db", json::stringify(
        object_t({ { "fid", list.first()->data.fid },
                   { "qid", list.first()->data.qid }
        }) ));

        state[1]=1; coWait( state[1]==1 ); coGoto(0); 

    gnStop
    }

};}

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { void run_v1_tcp_server(){

    auto tenv= process::env::get( "SOCK_TIMEOUT" );
    auto penv= process::env::get( "SOCK_PORT" );

    auto port= string::to_uint( penv );
    auto time= string::to_int ( tenv );
    auto tcp = tcp::server();

    /*.........................................................................*/

    tcp.onConnect([=]( socket_t cli ){

        auto tsk = cmd_task(); cli.set_timeout(time>0?time:0);
        auto ev = onSignalEvent.on([=]( string_t qid ){ tsk.emit( cli, qid ); });

        cli.onData ([=]( string_t data ){ tsk.append(cli,data); });
        tsk.onDrain([=](){ process::clear(ev); });
        cli.onDrain([=](){ tsk.close(); });

        process::poll::add(tsk);

    });

    /*.........................................................................*/

    tcp.listen( "localhost", port, [=](...){
        console::log( regex::format(
            "<> tcp://localhost:${0}", port
        ));
    });

}}

/*────────────────────────────────────────────────────────────────────────────*/

#undef PATTERN