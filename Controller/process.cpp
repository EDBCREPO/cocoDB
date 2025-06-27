#pragma once

/*────────────────────────────────────────────────────────────────────────────*/

namespace cocoDB { void run_v1_process() {
    
    process::task::add([=](){
    coStart ; coDelay( TIME_MINUTES( 30 ) );
        add_cmd( socket_t(), "flushall" );
    coGoto(0) ; coStop
    }); 

}}

/*────────────────────────────────────────────────────────────────────────────*/
