#include <nodepp/nodepp.h>
#include <nodepp/cluster.h>
#include <nodepp/crypto.h>
#include <sqlite/sqlite.h>
#include <nodepp/timer.h>
#include <nodepp/date.h>
#include <nodepp/tcp.h>
#include <nodepp/os.h>

using namespace nodepp;

#include "../Controller/import.cpp"

/*────────────────────────────────────────────────────────────────────────────*/

void onMain(){
    process::env::init(".env");
    cocoDB::run_v1_tcp_server();
    cocoDB::run_v1_process();
}

/*────────────────────────────────────────────────────────────────────────────*/
