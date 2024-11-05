#include <glog/logging.h>
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

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

using namespace csce662;

using google::protobuf::Empty;
using google::protobuf::Timestamp;

using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

typedef uint32_t ClusterID;
typedef uint32_t ServerID;

std::unordered_map<ClusterID,
                   std::unordered_map<ServerID, std::unique_ptr<ServerInfo>>>
    clusters;
std::unordered_map<ClusterID, std::unordered_map<ServerID, time_t>>
    server_status;

const std::time_t now() {
  return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

const ServerInfo* get_server(uint32_t client_id) {
  if (clusters.empty()) {
    log(ERROR, "No servers available");
    return nullptr;
  }
  auto& cluster = clusters[(client_id - 1) % clusters.size() + 1];
  return cluster.begin()->second.get();  // return first server in cluster
}

class CoordServiceImpl final : public CoordService::Service {
  Status Register(ServerContext* context,
                  const ServerRegistration* registration,
                  Empty* response) override {
    const ServerInfo& server_info = registration->info();
    log(INFO, "Register request from " + server_info.hostname() + ":" +
                  std::to_string(server_info.port()));
    log(INFO, "Cluster ID: " + std::to_string(registration->cluster_id()) +
                  " Server ID: " + std::to_string(server_info.id()));
    // make sure server is not already registered
    // if (clusters[registration->cluster_id()].find(server_info.id()) !=
    //     clusters[registration->cluster_id()].end()) {
    //   return Status(grpc::StatusCode::ALREADY_EXISTS, "Server already exists");
    // }
    // add server info to appropriate capability groups
    clusters[registration->cluster_id()][server_info.id()] =
        std::make_unique<ServerInfo>(server_info);
    return Status::OK;
  }

  Status Heartbeat(ServerContext* context, const HeartbeatMessage* message,
                   Empty*) override {
    log(INFO,
        "Heartbeat from cluster: " + std::to_string(message->cluster_id()) +
            " server: " + std::to_string(message->server_id()));
    server_status[message->cluster_id()][message->server_id()] = now();
    return Status::OK;
  }

  Status GetServer(ServerContext* context, const ClientID* clientid,
                   ServerInfo* response) override {
    log(INFO,
        "GetServer request from client id: " + std::to_string(clientid->id()));
    if (clusters.empty()) {
      return Status(grpc::StatusCode::NOT_FOUND, "No servers available");
    }
    auto* server = get_server(clientid->id());
    if (server == nullptr) {
      return Status(grpc::StatusCode::NOT_FOUND, "No servers available");
    }
    response->CopyFrom(*server);
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
  std::string port = "9090";
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
  std::string log_file_name = std::string("coordinator-") + ".log";
  google::InitGoogleLogging(log_file_name.c_str());
  RunServer(port);
  return 0;
}
