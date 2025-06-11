#define MAX_FILENO 10485760

/*────────────────────────────────────────────────────────────────────────────*/

#include <nodepp/nodepp.h>
#include <nodepp/cluster.h>
#include <sqlite/sqlite.h>
#include <nodepp/timer.h>
#include <apify/apify.h>
#include <nodepp/date.h>
#include <nodepp/tcp.h>
#include <nodepp/ws.h>
#include <nodepp/os.h>

using namespace nodepp;

queue_t<ws_t> ws_list ; ptr_t<ws_t> ws_client;
#include "../Controller/import.cpp"

/*────────────────────────────────────────────────────────────────────────────*/

void onMain(){
    process::env::init(".env");
    miniDB::run_v1_cluster();
}

/*────────────────────────────────────────────────────────────────────────────*/
