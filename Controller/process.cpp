#pragma once

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { void run_v1_process() { process::task::add([=](){
coStart ; coDelay( TIME_MINUTES( string::to_ulong(process::env::get("TMP_TIMEOUT")) ) );

    fs::read_folder( process::env::get("STORAGE_PATH"),[=]( string_t name ){
        auto cli = ws_list.get(); ws_list.next(); if( cli==nullptr ){ return; }
        apify::add( cli->data ).emit( "FLUSH", "/api/v1/db", name );
    });

coGoto(0) ; coStop
}); }}

/*────────────────────────────────────────────────────────────────────────────*/
