
#include <nodepp/nodepp.h>
#include "redis.h"

using namespace nodepp;

void onMain() {

    auto db = redis::tcp::add("db://localhost:3031");

    db.exec("PUSH FOO BAT1");
    db.exec("PUSH FOO BAT2");
    db.exec("PUSH FOO BAT3");
    db.exec("PUSH FOO BAT4");
    db.exec("PUSH FOO BAT5");
    db.exec("PUSH FOO BAT6");

    db.exec("RANGE FOO 0 -1",[]( string_t data ){
        console::log( "->", data );
    });

}
