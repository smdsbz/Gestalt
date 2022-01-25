/**
 * @file session_servicer.hpp
 */

#pragma once

#include "Session.pb.h"
#include "Session.grpc.pb.h"


namespace gestalt {

class Server;

namespace rpc {

using namespace grpc;
using namespace google::protobuf;


class SessionServicer final : public gestalt::rpc::Session::Service {

    gestalt::Server *const server;  // a reference to parent instance

public:
    SessionServicer(gestalt::Server *_s) noexcept : server(_s)
    { }

    Status Connect(ServerContext *ctx, const ClientProp *in, ServerWriter<MemoryRegion> *out) override;
    Status Disconnect(ServerContext *ctx, const ClientProp *in, Empty *out) override;
};  /* class SessionServicer */

}   /* namespace rpc */
}   /* namespace gestalt */
