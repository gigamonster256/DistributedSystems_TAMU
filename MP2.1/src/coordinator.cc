#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using namespace csce662;

using google::protobuf::Empty;
using google::protobuf::Timestamp;

using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

std::map<uint32_t, time_t> server_status;

const std::time_t now() {
  return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

class CoordServiceImpl final : public CoordService::Service {
  Status Register(ServerContext* context, const ServerRegistration* request,
                  Empty* response) override {
    return Status::OK;
  }

  Status Heartbeat(ServerContext* context, const ServerID* serverid,
                   Empty*) override {
    server_status[serverid->id()] = now();
    return Status::OK;
  }

  Status GetServer(ServerContext* context, const ClientID* clientid,
                   ServerInfo* response) override {
    return Status::OK;
  }
};

void RunServer(std::string port_no) {
  std::string server_address("127.0.0.1:" + port_no);
  CoordServiceImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server(builder.BuildAndStart());
  std::cout << "Coordinator listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1) {
    switch (opt) {
      case 'p':
        port = optarg;
        break;
      default:
        std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(port);
  return 0;
}
