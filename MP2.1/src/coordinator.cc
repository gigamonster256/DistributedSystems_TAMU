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

// cluster tracking information
struct SNSClusterInfo {
  size_t master_index;
  std::vector<ServerInfo> servers;
};

typedef std::map<ClusterID, SNSClusterInfo> SNSClusterMap;

SNSClusterMap clusters;

// heartbeat tracking information
std::map<ClusterID, std::map<ServerID, time_t>> server_status;

std::time_t now() {
  return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

const ServerInfo& get_server(uint32_t client_id) {
  // client and cluster id are 1-indexed
  auto cluster_index = ((client_id - 1) % 3) + 1;
  auto it = clusters.find(cluster_index);

  // if the requested cluster does not exist, return the first cluster
  if (it == clusters.end()) {
    it = clusters.begin();
  }
  auto& cluster = it->second;
  auto& servers = cluster.servers;
  auto& master = servers[cluster.master_index];
  return master;
}

ClusterStatus addToSNSCluster(ClusterID cluster_id,
                              const ServerInfo& server_info) {
  if (clusters.find(cluster_id) == clusters.end()) {
    log(INFO, "Creating new cluster: " + std::to_string(cluster_id));
    log(INFO, "New Master: " + server_info.hostname() + ":" +
                  std::to_string(server_info.port()));
    clusters[cluster_id].master_index = 0;
    clusters[cluster_id].servers.push_back(server_info);
    return ClusterStatus::MASTER;
  } else {
    // if the server is already in the cluster, replace it
    auto& servers = clusters[cluster_id].servers;
    for (auto& server : servers) {
      if (server.id() == server_info.id()) {
        log(INFO, "Replacing server: " + server.hostname() + ":" +
                      std::to_string(server.port()) + " with " +
                      server_info.hostname() + ":" +
                      std::to_string(server_info.port()));
        server = server_info;
        return ClusterStatus::SLAVE;
      }
    }
    log(INFO, "Adding server: " + server_info.hostname() + ":" +
                  std::to_string(server_info.port()));
    clusters[cluster_id].servers.push_back(server_info);
  }
  return ClusterStatus::SLAVE;
}

class CoordServiceImpl final : public CoordService::Service {
  Status Register(ServerContext*, const ServerRegistration* registration,
                  RegistrationResponse* response) override {
    auto cluster_id = registration->cluster_id();
    const ServerInfo& server_info = registration->info();
    auto& hostname = server_info.hostname();
    auto port = server_info.port();
    auto server_id = server_info.id();
    log(INFO, "Register request from " + hostname + ":" + std::to_string(port));
    log(INFO, "Cluster ID: " + std::to_string(cluster_id) +
                  " Server ID: " + std::to_string(server_id));

    // add server info to appropriate capability groups
    for (int idx = 0; idx < registration->capabilities_size(); idx++) {
      auto capability = registration->capabilities(idx);
      if (capability == ServerCapability::SNS) {
        response->set_status(addToSNSCluster(cluster_id, server_info));
      }
    }

    // count as heartbeat
    server_status[cluster_id][server_id] = now();
    return Status::OK;
  }

  Status Heartbeat(ServerContext*, const HeartbeatMessage* message,
                   Empty*) override {
    auto cluster_id = message->cluster_id();
    auto server_id = message->server_id();
    log(INFO, "Heartbeat from cluster: " + std::to_string(cluster_id) +
                  " server: " + std::to_string(server_id));
    server_status[cluster_id][server_id] = now();
    return Status::OK;
  }

  Status GetServer(ServerContext*, const ClientID* clientid,
                   ServerInfo* response) override {
    log(INFO,
        "GetServer request from client id: " + std::to_string(clientid->id()));
    if (clusters.empty()) {
      return Status(grpc::StatusCode::NOT_FOUND, "No servers available");
    }
    auto& server = get_server(clientid->id());
    response->CopyFrom(server);
    return Status::OK;
  }
};

void RunServer(uint32_t port_no) {
  std::string server_address = "127.0.0.1:" + std::to_string(port_no);
  CoordServiceImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server(builder.BuildAndStart());
  std::cout << "Coordinator listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {
  uint32_t port = 9090;
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1) {
    switch (opt) {
      case 'p':
        port = atoi(optarg);
        break;
      default:
        std::cerr << "Invalid Command Line Argument\n";
    }
  }
  std::string log_file_name = "coordinator.log";
  google::InitGoogleLogging(log_file_name.c_str());
  RunServer(port);
  return 0;
}
