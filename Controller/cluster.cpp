#pragma once

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { void cluster_reset( ptr_t<bool> state ) { try {

    if( process::is_child() ){ throw ""; }
    if( *state==false )      { throw ""; }

    auto pid = cluster::add();

    pid.onDrain.once([=](){
        if( *state==false ){ return; }
        cluster_reset( state );
    });

} catch(...) {} }}

/*────────────────────────────────────────────────────────────────────────────*/

namespace miniDB { void run_v1_cluster() { try {
    if( process::is_child() ){ throw ""; }
    ptr_t<bool> state =new bool( true );

    run_v1_ws_server(); run_v1_process();

    process::onSIGEXIT([=](){ *state=false; }); timer::timeout([=](){
        auto   x = min( os::cpus(),string::to_uint(process::env::get("N_CPU")) );
        while( x-->0 ){ cluster_reset(state); }
    },1000);

} catch(...) { run_v1_ws_client(); run_v1_tcp_server(); } }}

/*────────────────────────────────────────────────────────────────────────────*/
