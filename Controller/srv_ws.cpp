#pragma once

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { apify_host_t<ws_t> run_v1_ws_server_routine(){
    apify_host_t<ws_t> app; queue_t<object_t> list;
    auto flush = type::bind( new bool(false) );
    map_t<string_t,string_t> queue;

    app.on("WIPE","/api/v1/db",[=]( apify_t<ws_t> cli ){ *flush=true; });

    app.on("UNLOCK","/api/v1/db",[=]( apify_t<ws_t> cli ){ try {
        auto obj = json::parse( cli.message );
        queue.erase( obj["fid"].as<string_t>() );
        cli.emit( cli.method,cli.path,cli.message);
    } catch(...) {} cli.done(); });

    app.on("LOCK","/api/v1/db",[=]( apify_t<ws_t> cli ){ try {
        auto obj = json::parse( cli.message );
        list.push( object_t({
            { "cli", cli.get_fd() },
            { "fid", obj["fid"]   },
            { "qid", obj["qid"]   }
        }) );
    } catch(...) {} cli.done(); });

    process::poll::add([=](){
    coStart

        if( *flush ){ coWait( !queue.empty() ); os::exec( regex::format(
            "rm -R ./${0}/*", process::env::get("STORAGE_PATH")
        )); *flush=false; }

        do{ auto n = list.get(); if( n == nullptr )   {                break; }
        if( n->data["cli"].as<ws_t>().is_closed() )   { list.erase(n); break; }
        if( queue.has(n->data["fid"].as<string_t>()) ){ list.next();   break; }

            apify_t<ws_t>( n->data["cli"].as<ws_t>() ).emit(
                "LOCK", "/api/v1/db", json::stringify(
                object_t({ { "fid", n->data["fid"] },
                           { "qid", n->data["qid"] }
                })
            ));

        queue[n->data["fid"].as<string_t>()]=nullptr;
        list.erase(n); } while(0);

    coGoto(0) ; coStop
    });

    return app;
}}

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { void run_v1_ws_server(){

    auto penv= process::env::get( "APIF_PORT" );
    auto port= string::to_uint( penv );
    auto app = apify::add<ws_t>();
    auto ws  = ws::server();

    /*.........................................................................*/

    app.add( run_v1_ws_server_routine() );

    /*.........................................................................*/

    ws.onConnect([=]( ws_t cli ){
        ws_list.push( cli ); auto ID = ws_list.last();
        cli.onDrain([=](...){ ws_list.erase( ID ); });
        cli.onData ([=]( string_t data ){ app.next( cli, data ); });
    });

    /*.........................................................................*/

    ws.listen( "localhost", port, [=](...){
        console::log( regex::format(
            "<> ws://localhost:${0}", port
        ));
    });

}}

/*────────────────────────────────────────────────────────────────────────────*/
