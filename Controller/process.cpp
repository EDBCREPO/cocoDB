#pragma once

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { void run_v1_process() { process::task::add([=](){
coStart ; coDelay( TIME_HOURS(3) );

    fs::read_folder( process::env::get("STORAGE_PATH"),[=]( string_t name ){
        auto cli = ws_list.get(); ws_list.next(); if( cli==nullptr ){ return; }
        apify::add( cli->data ).emit( "FLUSH", "/api/v1/db", name );
    });

coGoto(0) ; coStop
}); }}

/*────────────────────────────────────────────────────────────────────────────*/
