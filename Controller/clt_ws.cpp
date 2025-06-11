#pragma once

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { apify_host_t<ws_t> run_v1_ws_client_routine(){
    apify_host_t<ws_t> app;

    app.on("FLUSH","/api/v1/db",[=]( apify_t<ws_t> cli ){ try {

        auto tsk=cmd_task(); auto skt=type::bind( socket_t() );
        auto cmd=regex::format( "flush ${0}", cli.message );
        tsk.append ( *skt, cmd ); process::poll::add( tsk ); 
        
        auto ev =onSignalEvent.on([=]( string_t qid ){
             tsk.emit( *skt, qid ); tsk.close();
        });

        tsk.onDrain([=](){ process::clear(ev); });

    } catch(...) { } cli.done(); });

    app.on("LOCK","/api/v1/db",[=]( apify_t<ws_t> cli ){ try {

        auto obj = json::parse( cli.message );
        onSignalEvent.emit( obj["qid"].as<string_t>() );

    } catch(...) { } cli.done(); });

    return app;
}}

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { void run_v1_ws_client(){ try {

    auto penv= process::env::get( "APIF_PORT" );
    auto port= string::to_uint( penv );
    auto app = apify::add<ws_t>();

    /*.........................................................................*/

    auto cli = ws::client( regex::format( "ws://localhost:${0}", port ));

    /*.........................................................................*/

    app.add( run_v1_ws_client_routine() );

    /*.........................................................................*/

    cli.onConnect([=]( ws_t cli ){ ws_client = type::bind( cli );
    cli.onData .on  ([=]( string_t data ){ app.next( cli, data ); });
    cli.onDrain.once([=](...){ timer::timeout([=](){ run_v1_ws_client(); },1000); }); });
    cli.onError.once([=](...){ timer::timeout([=](){ run_v1_ws_client(); },1000); });

} catch(...) {
    timer::timeout([=](){ run_v1_ws_client(); },1000);
}}}

/*────────────────────────────────────────────────────────────────────────────*/
