/*
 * Copyright 2023 The Nodepp Project Authors. All Rights Reserved.
 *
 * Licensed under the MIT (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://github.com/NodeppOficial/nodepp/blob/main/LICENSE
 */

/*────────────────────────────────────────────────────────────────────────────*/

#ifndef NODEPP_REDIS_TCP
#define NODEPP_REDIS_TCP

/*────────────────────────────────────────────────────────────────────────────*/

#include <nodepp/nodepp.h>
#include <nodepp/promise.h>
#include <nodepp/stream.h>
#include <nodepp/tcp.h>
#include <nodepp/url.h>

/*────────────────────────────────────────────────────────────────────────────*/

#ifndef NODEPP_REDIS_GENERATOR
#define NODEPP_REDIS_GENERATOR

namespace nodepp { namespace _redis_ { GENERATOR( stream ){
protected:
    _file_::write write; _file_::read read;
    string_t raw, data;  ptr_t<ulong> pos;
    _file_::line  line;  ulong time=0;
public:

    template< class V, class U >
    coEmit( string_t cmd, const V& cb, const U& self ){ auto fd = self->get_fd();
        if( fd.is_closed() )                               { self->release(); return -1; }
        if( time>0&&(process::now()-time)>TIME_SECONDS(1) ){ self->release(); return -1; }
    gnStart

        coWait( self->is_used() ==1 ); self->use(); time=process::now();
        coWait( write( &fd,cmd )==1 );
            if( write.state     ==0 ){ coGoto (2); }
        pos = ptr_t<ulong> ({ 1, 0 }); coYield(1);  time=process::now();

        coWait( line( &fd )==1 );
            if( line.state ==0 ){ coGoto(2); } raw = line.data;

        if( regex::test( raw, "[$*]-1",true ) )      { coGoto(2); }
        if( regex::test( raw, "^[+]" )||raw.empty() ){ coGoto(2); }
        if(!regex::test( raw, "[$*:]-?\\d+" ) )      { process::error( raw.slice(0,-2) ); coGoto(2); }

        if( regex::test( raw, "[*]\\d+" ) ){
            pos[0] = string::to_ulong( regex::match( raw, "\\d+" ) );
        if( pos[0] == 0 ){ coGoto(2); } coGoto(1);
        } elif( regex::test ( raw, "[$]\\d+" ) ) {
            pos[1] = string::to_ulong( regex::match( raw, "\\d+" ) ) + 2;
        } elif( regex::test ( raw, "[:]\\d+" ) ) {
            cb( regex::match( raw, "\\d+" ) ); coGoto(2);
        }

        while( pos[0]-->0 )           { data.clear();
        while( data.size() != pos[1] ){
        coWait( read( &fd, pos[1]-data.size() )==1 );
            if( read.state==0 ){ coGoto(2); } data+=read.data;
        }   cb( data.slice(0,-2) );

        if( pos[0]!=0 ) { coGoto(1); }}
        coYield(2); self->release();

    gnStop
    }

    template< class U >
    coEmit( string_t cmd, const U& self ){ auto fd = self->get_fd();
        if( fd.is_closed() )                               { self->release(); return -1; }
        if( time>0&&(process::now()-time)>TIME_SECONDS(1) ){ self->release(); return -1; }
    gnStart

        coWait( self->is_used() ==1 ); self->use();     time=process::now();
        coWait( write( &fd,cmd )==1 ); self->release();

    gnStop
    }

};}}

#endif

/*────────────────────────────────────────────────────────────────────────────*/

namespace nodepp { class redis_tcp_t {
protected:

    struct NODE {
        bool used =0;
        bool state=0;
        socket_t  fd;
    };  ptr_t<NODE> obj;

public:

    event_t<> onUse;
    event_t<> onRelease;

    /*─······································································─*/

   ~redis_tcp_t () noexcept { if( obj.count()>1 ) { return; } free(); }
    redis_tcp_t ( socket_t cli ) : obj( new NODE ) { set_fd(cli); }
    redis_tcp_t () : obj( new NODE ) {}

    /*─······································································─*/

    void set_fd( socket_t cli ) const noexcept { obj->fd=cli; obj->state=1;     }
    socket_t&  get_fd()         const noexcept { return obj->fd;                }
    bool is_available()         const noexcept { return obj->fd.is_available(); }
    bool is_closed()            const noexcept { return obj->fd.is_closed();    }

    /*─······································································─*/

    void use()     const noexcept { if( obj->used==1 ){ return; } obj->used=1; onUse    .emit(); }
    void release() const noexcept { if( obj->used==0 ){ return; } obj->used=0; onRelease.emit(); }
    bool is_used() const noexcept { return obj->used; }
    void close()   const noexcept { obj->fd.close(); }

    /*─······································································─*/

    array_t<string_t> exec( const string_t& cmd ) const {
        if( cmd.empty() || obj->state==0 || obj->fd.is_closed() ) { return nullptr; }
        queue_t<string_t> res; auto self = type::bind(this); _redis_::stream task;
        function_t<void,string_t> cb([&]( string_t data ){ res.push(data); });
        process::await( task, cmd+"\n", cb, self ); return res.data();
    }

    void exec( const string_t& cmd, const function_t<void,string_t>& cb ) const {
        if( cmd.empty() || obj->state==0 || obj->fd.is_closed() ) { return; }
        auto self = type::bind(this); _redis_::stream task;
        process::poll::add( task, cmd+"\n", cb, self );
    }

    void emit( const string_t& cmd ) const {
        if( cmd.empty() || obj->state==0 || obj->fd.is_closed() ) { return; }
        auto self = type::bind(this); _redis_::stream task;
        function_t<void,string_t> cb([&]( string_t ){});
        process::poll::add( task, cmd+"\n", cb, self );
    }

    /*─······································································─*/

    void send( const string_t& cmd ) const {
        if( cmd.empty() || obj->state==0 || obj->fd.is_closed() ) { return; }
        auto self = type::bind(this); _redis_::stream task;
        process::await( task, cmd+"\n", self );
    }

    /*─······································································─*/

    virtual void free() const noexcept {
        if( obj->state == 0 ){ return; }
            obj->state  = 0; obj->fd.free(); 
        release(); onUse.clear(); onRelease.clear();
    }

};}

/*────────────────────────────────────────────────────────────────────────────*/

namespace nodepp { namespace redis { namespace tcp {

    promise_t<redis_tcp_t,except_t> connect( const string_t& uri ) {
    return promise_t<redis_tcp_t,except_t> ([=]( function_t<void,redis_tcp_t> res, function_t<void,except_t> rej ){
        if( !url::is_valid( uri ) ){ rej( except_t("Invalid Redis URL") ); return; }

        auto rdis = type::bind( new redis_tcp_t() );
        auto host = url::hostname( uri );
        auto port = url::port( uri );
        auto auth = url::auth( uri );
        auto user = url::user( uri );
        auto pass = url::pass( uri );
        auto Auth = string_t();

        if( !user.empty() && !pass.empty() ){
            Auth = string::format("AUTH %s %s", user.get(), pass.get() );
        } elif( !auth.empty() ) {
            Auth = string::format("AUTH %s", auth.get() );
        }

        auto client = tcp_t ([=]( socket_t cli ){
             rdis->set_fd( cli );  if( !Auth.empty() )
           { rdis->exec( Auth ); } res(*rdis); return;
        });

        client.onError([=]( except_t error ){ rej(error); });
        client.connect( host, port );

    }); }

    /*─······································································─*/

    template<class...T>
    redis_tcp_t await( const T&... args ) {
        auto raw = connect( args... ).await();
        if( !raw.has_value() ){ throw raw.error(); } return raw.value();
    }

    /*─······································································─*/

    template<class...T>
    redis_tcp_t add( const T&... args ) {
        auto raw = connect( args... ).await();
        if( !raw.has_value() ){ throw raw.error(); } return raw.value();
    }

}}}

/*────────────────────────────────────────────────────────────────────────────*/

#ifndef REDIS_FORMAT
#define REDIS_FORMAT
namespace nodepp { namespace redis {
    template< class V, class... T >
string_t format( const V& argc, const T&... args ){
    string_t result = string::to_string(argc); ulong n=0;

    string::map([&]( const string_t arg ){
        if( arg.empty() || result.empty() ){ return; }
        if( regex::test(arg,"[<\'\">]|\\N") ){ result=nullptr; return; }
        string_t reg = "\\$\\{" + string::to_string(n) + "\\}";
        result = regex::replace_all( result, reg, arg ); n++;
    },  args... );

    return result;
}}}
#endif

/*────────────────────────────────────────────────────────────────────────────*/

#endif
