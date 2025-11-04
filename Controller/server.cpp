#pragma once
#define PATTERN "\"([^\"]+)\"|\'([^\']+)\'|([^ \r\n\t]+)"

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { void add_cmd( socket_t cli, string_t cmd ){ try {
    if( cmd.empty() || cli.is_closed() ){ throw ""; }

    auto match = regex::get_memory( cmd, PATTERN );
    auto list  = queue_t<cmd_t>();

      if( match.empty() ) { throw ""; } while( !match.empty() ) {
      if( regex::test(match[0],"mcount"  ,true) ){ cmd_match_count(match,list);  continue; }
    elif( regex::test(match[0],"mrange"  ,true) ){ cmd_match_range(match,list);  continue; }
    elif( regex::test(match[0],"expire"  ,true) ){ cmd_expire(match,list);       continue; }
    elif( regex::test(match[0],"match"   ,true) ){ cmd_match(match,list);        continue; }
    elif( regex::test(match[0],"flush"   ,true) ){ cmd_flush(match,list);        continue; }
    elif( regex::test(match[0],"mdel"    ,true) ){ cmd_match_delete(match,list); continue; }
    elif( regex::test(match[0],"exec"    ,true) ){ cmd_next(match,list);         continue; }
    elif( regex::test(match[0],"quit"    ,true) ){ cmd_next(match,list);         continue; }
    elif( regex::test(match[0],"del"     ,true) ){ cmd_delete(match,list);       continue; }
    elif( regex::test(match[0],"get"     ,true) ){ cmd_get(match,list);          continue; }
    elif( regex::test(match[0],"set"     ,true) ){ cmd_set(match,list);          continue; }
    elif( regex::test(match[0],"ttl"     ,true) ){ cmd_ttl(match,list);          continue; }
    elif( regex::test(match[0],"ex"      ,true) ){ cmd_shortex(match,list);      continue; }
    elif( regex::test(match[0],"inc"     ,true) ){ cmd_increase(match,list);     continue; }
    elif( regex::test(match[0],"dec"     ,true) ){ cmd_decrease(match,list);     continue; }
    elif( regex::test(match[0],"pop"     ,true) ){ cmd_pop(match,list);          continue; }
    elif( regex::test(match[0],"push"    ,true) ){ cmd_push(match,list);         continue; }
    elif( regex::test(match[0],"trim"    ,true) ){ cmd_trim(match,list);         continue; }
    elif( regex::test(match[0],"shift"   ,true) ){ cmd_shift(match,list);        continue; }
    elif( regex::test(match[0],"count"   ,true) ){ cmd_count(match,list);        continue; }
    elif( regex::test(match[0],"range"   ,true) ){ cmd_range(match,list);        continue; }
    elif( regex::test(match[0],"multi"   ,true) ){ cmd_next(match,list);         continue; }
    elif( regex::test(match[0],"flushall",true) ){ cmd_flushall(match,list);     continue; } throw ""; }

    auto task = _task_(); process::add( task, list, cli );

} catch(...) { cli.write( "-something went wrong\r\n" ); cli.close(); } }}

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { void run_v1_tcp_server(){

    auto tenv= process::env::get( "SOCK_TIMEOUT" );
    auto penv= process::env::get( "SOCK_PORT" );

    auto port= string::to_uint( penv );
    auto time= string::to_int ( tenv );
    auto tcp = tcp   ::server ();

    /*.........................................................................*/

    tcp.onConnect ([=]( socket_t cli ){ auto fd = cli.get_fd();
        cli.onData([=]( string_t cmd ){ add_cmd( cli, cmd ); });
        cli.set_timeout( time>0 ? time : 0 );
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
