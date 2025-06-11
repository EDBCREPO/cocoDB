#include <nodepp/nodepp.h>
#include <nodepp/tcp.h>

using namespace nodepp;

void onMain() {

    auto skt = tcp::client();

    skt.onConnect([=]( socket_t cli ){

        cli.onData([=]( string_t data ){
            console::log( "<>", data );
        });

        cli.write( "get hello" );
        console::log( "Connected" );

    });

    skt.connect( "localhost", 3031 );

}
