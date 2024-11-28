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
#include "proto_helpers.h"

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

using namespace csce662;

using google::protobuf::Empty;
using google::protobuf::Timestamp;

using grpc::ClientContext;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

typedef int32_t ClusterID;
typedef int32_t ServerID;

// cluster tracking information

typedef std::pair<ServerInfo, time_t> ServerInfoTimePair;
typedef struct {
  ServerID master_id;
  std::map<ServerID, std::pair<ServerInfoTimePair, ServerInfoTimePair>> servers;
} SNSClusterInfo;
typedef std::map<ClusterID, SNSClusterInfo> SNSClusterMap;

std::time_t now() {
  return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

class CoordServiceImpl final : public CoordService::Service {
  Status Register(ServerContext*, const ServerRegistration* registration,
                  RegistrationResponse* response) override;

  Status Heartbeat(ServerContext*, const HeartbeatMessage* message,
                   Empty*) override;

  Status GetServer(ServerContext*, const ClientID* clientid,
                   ServerInfo* response) override;

 private:
  SNSClusterMap clusters;

  const ServerInfo& get_server(const int32_t client_id) const;
  const ServerInfo& getMaster(const ClusterID cluster_id) const;
  bool is_master(const ClusterID cluster_id, const ServerID server_id) const;
  ClusterStatus addToSNSCluster(ClusterID cluster_id,
                                const ServerInfo& server_info);
  ClusterStatus addToSyncronizerCluster(const ServerInfo& server_info);
  time_t get_last_server_heartbeat(const ClusterID cluster_id,
                                   const ServerID server_id) const;
  void informNewSlave(const ClusterID cluster_id,
                      const ServerInfo& server_info);
  void notifyNewMasterSyncronizer(int cluster_id);
};

Status CoordServiceImpl::Register(ServerContext*,
                                  const ServerRegistration* registration,
                                  RegistrationResponse* response) {
  const auto cluster_id = registration->cluster_id();
  const ServerInfo& server_info = registration->info();
  const auto [hostname, port] = get_server_address(server_info);
  const auto server_id = server_info.id();

  const auto server_address = hostname + ":" + std::to_string(port);

  log(INFO, "Register request from " + server_address);
  log(INFO, "Cluster ID: " + std::to_string(cluster_id) +
                " Server ID: " + std::to_string(server_id));

  // add server info to appropriate capability groups
  for (int idx = 0; idx < registration->capabilities_size(); idx++) {
    const auto capability = registration->capabilities(idx);
    if (capability == ServerCapability::SNS) {
      // add to cluster
      const auto status = addToSNSCluster(cluster_id, server_info);
      response->set_status(status);

      // inform the master of the new slave
      if (status == ClusterStatus::SLAVE) {
        informNewSlave(cluster_id, server_info);
      }
    }
    if (capability == ServerCapability::SNS_SYNCHRONIZER) {
      // add to cluster
      const auto status = addToSyncronizerCluster(server_info);
      response->set_status(status);
    }
  }
  return Status::OK;
}

Status CoordServiceImpl::Heartbeat(ServerContext*,
                                   const HeartbeatMessage* message, Empty*) {
  auto cluster_id = message->cluster_id();
  auto server_id = message->server_id();
  // log(INFO, "Heartbeat from cluster: " + std::to_string(cluster_id) +
  //               " server: " + std::to_string(server_id));
  // syncronizers show as cluster 99
  if (cluster_id == 99) {
    // derive cluster id from server id
    cluster_id = (server_id + 1) / 2;
    server_id = (server_id - 1) % 2 + 1;
    clusters[cluster_id].servers[server_id].second.second = now();
  } else {
    clusters[cluster_id].servers[server_id].first.second = now();
  }
  return Status::OK;
}

Status CoordServiceImpl::GetServer(ServerContext*, const ClientID* clientid,
                                   ServerInfo* response) {
  log(INFO,
      "GetServer request from client id: " + std::to_string(clientid->id()));
  if (clusters.empty()) {
    log(ERROR, "No servers available");
    return Status(grpc::StatusCode::NOT_FOUND, "No servers available");
  }
  auto& server = get_server(clientid->id());
  response->CopyFrom(server);
  return Status::OK;
}

const ServerInfo& CoordServiceImpl::get_server(const int32_t client_id) const {
  // client and cluster id are 1-indexed
  auto cluster_index = (client_id - 1) % 3 + 1;

  return getMaster(cluster_index);
}

const ServerInfo& CoordServiceImpl::getMaster(
    const ClusterID cluster_id) const {
  auto it = clusters.find(cluster_id);

  // if cluster not found, throw exception
  if (it == clusters.end()) {
    log(ERROR, "Cluster: " + std::to_string(cluster_id) + " not found");
    throw std::runtime_error("Cluster not found");
  }
  const auto master_id = it->second.master_id;
  return it->second.servers.at(master_id).first.first;
}

bool CoordServiceImpl::is_master(const ClusterID cluster_id,
                                 const ServerID server_id) const {
  const auto it = clusters.find(cluster_id);
  if (it == clusters.end()) {
    return false;
  }
  return it->second.master_id == server_id;
}

std::mutex add_to_sns_mutex;
ClusterStatus CoordServiceImpl::addToSNSCluster(ClusterID cluster_id,
                                                const ServerInfo& server_info) {
  std::lock_guard<std::mutex> lock(add_to_sns_mutex);
  const auto [hostname, port] = get_server_address(server_info);
  const auto server_id = server_info.id();
  const auto server_address = hostname + ":" + std::to_string(port);
  // log(INFO, "Register request from " + server_address);

  auto it = clusters.find(cluster_id);

  // new cluster
  if (it == clusters.end()) {
    it = clusters.insert({cluster_id, SNSClusterInfo()}).first;
  }

  auto& cluster = it->second;

  // new master
  if (cluster.servers.empty()) {
    cluster.master_id = server_id;
  }

  cluster.servers[server_id].first = {server_info, now()};

  const auto is_master = server_id == cluster.master_id;

  if (is_master) {
    // start thread to check for dead servers and tell the backup syncronizer to
    // take over
    std::thread([this, cluster_id]() {
      while (true) {
        sleep(5);
        // log(INFO, "Checking for dead master server in cluster " +
        //               std::to_string(cluster_id));
        auto it = clusters.find(cluster_id);
        if (it == clusters.end()) {
          return;
        }
        auto& cluster = it->second;
        auto& servers = cluster.servers;
        auto master_id = cluster.master_id;
        auto master_heartbeat = servers[master_id].first.second;
        auto now = std::chrono::system_clock::to_time_t(
            std::chrono::system_clock::now());
        if (now - master_heartbeat > 10) {
          log(ERROR, "Master server for cluster " + std::to_string(cluster_id) +
                         " is dead");
          notifyNewMasterSyncronizer(cluster_id);
        } else {
          // log(INFO, "Master server for cluster " + std::to_string(cluster_id)
          // +
          //               " is alive for " +
          //               std::to_string(now - master_heartbeat) + " seconds");
        }
      }
    }).detach();
  }

  return is_master ? ClusterStatus::MASTER : ClusterStatus::SLAVE;
}

std::mutex sync_add_mutex;
ClusterStatus CoordServiceImpl::addToSyncronizerCluster(
    const ServerInfo& server_info) {
  std::lock_guard<std::mutex> lock(sync_add_mutex);
  const auto [hostname, port] = get_server_address(server_info);
  const auto sync_id = server_info.id();
  // compute cluster id
  const auto cluster_id = (sync_id + 1) / 2;
  const auto server_id = (sync_id - 1) % 2 + 1;
  const auto server_address = hostname + ":" + std::to_string(port);
  // log(INFO, "Register request from " + server_address);
  clusters[cluster_id].servers[server_id].second = {server_info, now()};

  // 1, 3, 5 always start as master
  return sync_id % 2 == 1 ? ClusterStatus::MASTER : ClusterStatus::SLAVE;
}

time_t CoordServiceImpl::get_last_server_heartbeat(
    const ClusterID cluster_id, const ServerID server_id) const {
  const auto it = clusters.find(cluster_id);
  if (it == clusters.end()) {
    return 0;
  }
  const auto& cluster = it->second;
  const auto server_it = cluster.servers.find(server_id);
  if (server_it == cluster.servers.end()) {
    return 0;
  }
  return server_it->second.first.second;
}

void CoordServiceImpl::notifyNewMasterSyncronizer(int cluster_id) {
  // get the backup syncronizer
  auto it = clusters.find(cluster_id);
  if (it == clusters.end()) {
    log(ERROR, "Cluster: " + std::to_string(cluster_id) + " not found");
    return;
  }
  auto& cluster = it->second;
  auto& servers = cluster.servers;
  // set the the backup master
  auto old_master_sync_info = servers[cluster.master_id].second.first;

  cluster.master_id = (cluster.master_id % 2) + 1;
  auto& master_sync = servers[cluster.master_id].second.first;

  auto [old_hostname, old_port] = get_server_address(old_master_sync_info);
  auto old_master_sync_address = old_hostname + ":" + std::to_string(old_port);
  std::cout << "Old Master Syncronizer Address: " << old_master_sync_address
            << std::endl;

  auto [hostname, port] = get_server_address(master_sync);
  auto new_master_sync_address = hostname + ":" + std::to_string(port);
  std::cout << "New Master Syncronizer Address: " << new_master_sync_address
            << std::endl;

  std::cerr << "Master syncronizer address: " << new_master_sync_address
            << std::endl;

  auto old_master_stub = SynchronizerService::NewStub(grpc::CreateChannel(
      old_master_sync_address, grpc::InsecureChannelCredentials()));
  auto new_master_stub = SynchronizerService::NewStub(grpc::CreateChannel(
      new_master_sync_address, grpc::InsecureChannelCredentials()));

  // send the SetStatus to slave to old master

  ClientContext context;
  Empty response;
  RegistrationResponse request;
  request.set_status(SLAVE);

  auto status = old_master_stub->SetStatus(&context, request, &response);
  if (!status.ok()) {
    log(ERROR,
        "SetStatus: Failed to inform old master syncronizer of new master");
    log(ERROR, status.error_message());
    log(ERROR, status.error_code());
  } else {
    log(INFO, "Notified old master syncronizer");
  }

  ClientContext context2;
  request.set_status(MASTER);

  status = new_master_stub->SetStatus(&context2, request, &response);
  if (!status.ok()) {
    log(ERROR, "SetStatus: Failed to inform new master syncronizer");
    log(ERROR, status.error_message());
    log(ERROR, status.error_code());
  } else {
    log(INFO, "Notified new master syncronizer");
  }
}

void CoordServiceImpl::informNewSlave(const ClusterID cluster_id,
                                      const ServerInfo& server_info) {
  // get the master for the cluster
  const auto& master = getMaster(cluster_id);

  // inform the master of the new slave
  auto [hostname, port] = get_server_address(master);
  auto master_address = hostname + ":" + std::to_string(port);

  auto stub = ReplicatorService::NewStub(
      grpc::CreateChannel(master_address, grpc::InsecureChannelCredentials()));

  // send the new slave info to the master
  Empty response;
  ClientContext context;
  auto status = stub->InformNewSlave(&context, server_info, &response);

  if (!status.ok()) {
    log(ERROR, "InformNewSlave: Failed to inform master of new slave");
  }
}

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
