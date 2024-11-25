#include <fcntl.h>
#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <json/json.h>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

using namespace csce662;

using google::protobuf::Empty;
using google::protobuf::Timestamp;

using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

#define SYNC_CLUSTER_NUM 99

class SynchronizerRabbitMQ {
 private:
  amqp_connection_state_t conn;
  amqp_channel_t channel;
  std::string hostname;
  int port;
  int synchID;

  void setupRabbitMQ() {
    conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, hostname.c_str(), port);
    amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest",
               "guest");
    amqp_channel_open(conn, channel);
  }

  void declareQueue(const std::string &queueName) {
    amqp_queue_declare(conn, channel, amqp_cstring_bytes(queueName.c_str()), 0,
                       0, 0, 0, amqp_empty_table);
  }

  void publishMessage(const std::string &queueName,
                      const std::string &message) {
    amqp_basic_publish(conn, channel, amqp_empty_bytes,
                       amqp_cstring_bytes(queueName.c_str()), 0, 0, NULL,
                       amqp_cstring_bytes(message.c_str()));
  }

  std::string consumeMessage(const std::string &queueName,
                             int timeout_ms = 5000) {
    amqp_basic_consume(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                       amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(conn);

    struct timeval timeout;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;

    amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

    if (res.reply_type != AMQP_RESPONSE_NORMAL) {
      return "";
    }

    std::string message(static_cast<char *>(envelope.message.body.bytes),
                        envelope.message.body.len);
    amqp_destroy_envelope(&envelope);
    return message;
  }

 public:
  SynchronizerRabbitMQ(const std::string &host, int p, int id)
      : channel(1), hostname(host), port(p), synchID(id) {
    setupRabbitMQ();
    declareQueue("synch" + std::to_string(synchID) + "_users_queue");
    declareQueue("synch" + std::to_string(synchID) +
                 "_clients_relations_queue");
    declareQueue("synch" + std::to_string(synchID) + "_timeline_queue");
    // TODO: add or modify what kind of queues exist in your clusters based on
    // your needs
  }

  void publishUserList() {
    std::vector<std::string> users = get_all_users_func(synchID);
    std::sort(users.begin(), users.end());
    Json::Value userList;
    for (const auto &user : users) {
      userList["users"].append(user);
    }
    Json::FastWriter writer;
    std::string message = writer.write(userList);
    publishMessage("synch" + std::to_string(synchID) + "_users_queue", message);
  }

  void consumeUserLists() {
    std::vector<std::string> allUsers;
    // YOUR CODE HERE

    // TODO: while the number of synchronizers is harcorded as 6 right now, you
    // need to change this to use the correct number of follower synchronizers
    // that exist overall accomplish this by making a gRPC request to the
    // coordinator asking for the list of all follower synchronizers registered
    // with it
    for (int i = 1; i <= 6; i++) {
      std::string queueName = "synch" + std::to_string(i) + "_users_queue";
      std::string message =
          consumeMessage(queueName, 1000);  // 1 second timeout
      if (!message.empty()) {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(message, root)) {
          for (const auto &user : root["users"]) {
            allUsers.push_back(user.asString());
          }
        }
      }
    }
    updateAllUsersFile(allUsers);
  }

  void publishClientRelations() {
    Json::Value relations;
    std::vector<std::string> users = get_all_users_func(synchID);

    for (const auto &client : users) {
      int clientId = std::stoi(client);
      std::vector<std::string> followers = getFollowersOfUser(clientId);

      Json::Value followerList(Json::arrayValue);
      for (const auto &follower : followers) {
        followerList.append(follower);
      }

      if (!followerList.empty()) {
        relations[client] = followerList;
      }
    }

    Json::FastWriter writer;
    std::string message = writer.write(relations);
    publishMessage(
        "synch" + std::to_string(synchID) + "_clients_relations_queue",
        message);
  }

  void consumeClientRelations() {
    std::vector<std::string> allUsers = get_all_users_func(synchID);

    // YOUR CODE HERE

    // TODO: hardcoding 6 here, but you need to get list of all synchronizers
    // from coordinator as before
    for (int i = 1; i <= 6; i++) {
      std::string queueName =
          "synch" + std::to_string(i) + "_clients_relations_queue";
      std::string message =
          consumeMessage(queueName, 1000);  // 1 second timeout

      if (!message.empty()) {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(message, root)) {
          for (const auto &client : allUsers) {
            std::string followerFile =
                "./cluster_" + std::to_string(clusterID) + "/" +
                clusterSubdirectory + "/" + client + "_followers.txt";
            std::string semName = "/" + std::to_string(clusterID) + "_" +
                                  clusterSubdirectory + "_" + client +
                                  "_followers.txt";
            sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);

            std::ofstream followerStream(
                followerFile, std::ios::app | std::ios::out | std::ios::in);
            if (root.isMember(client)) {
              for (const auto &follower : root[client]) {
                if (!file_contains_user(followerFile, follower.asString())) {
                  followerStream << follower.asString() << std::endl;
                }
              }
            }
            sem_close(fileSem);
          }
        }
      }
    }
  }

  // for every client in your cluster, update all their followers' timeline
  // files by publishing your user's timeline file (or just the new updates in
  // them)
  //  periodically to the message queue of the synchronizer responsible for that
  //  client
  void publishTimelines() {
    std::vector<std::string> users = get_all_users_func(synchID);

    for (const auto &client : users) {
      int clientId = std::stoi(client);
      int client_cluster = ((clientId - 1) % 3) + 1;
      // only do this for clients in your own cluster
      if (client_cluster != clusterID) {
        continue;
      }

      std::vector<std::string> timeline = get_tl_or_fl(synchID, clientId, true);
      std::vector<std::string> followers = getFollowersOfUser(clientId);

      // for (const auto &follower : followers) {
      //   // send the timeline updates of your current user to all its
      //   followers

      //   // YOUR CODE HERE
      // }
    }
  }

  // For each client in your cluster, consume messages from your timeline queue
  // and modify your client's timeline files based on what the users they follow
  // posted to their timeline
  void consumeTimelines() {
    std::string queueName =
        "synch" + std::to_string(synchID) + "_timeline_queue";
    std::string message = consumeMessage(queueName, 1000);  // 1 second timeout

    if (!message.empty()) {
      // consume the message from the queue and update the timeline file of the
      // appropriate client with the new updates to the timeline of the user it
      // follows

      // YOUR CODE HERE
    }
  }

 private:
  void updateAllUsersFile(const std::vector<std::string> &users) {
    std::string usersFile = "./cluster_" + std::to_string(clusterID) + "/" +
                            clusterSubdirectory + "/all_users.txt";
    std::string semName = "/" + std::to_string(clusterID) + "_" +
                          clusterSubdirectory + "_all_users.txt";
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);

    std::ofstream userStream(usersFile,
                             std::ios::app | std::ios::out | std::ios::in);
    for (std::string user : users) {
      if (!file_contains_user(usersFile, user)) {
        userStream << user << std::endl;
      }
    }
    sem_close(fileSem);
  }
};

void Heartbeat(std::unique_ptr<CoordService::Stub> coordinator_stub,
               const uint32_t server_id) {
  while (true) {
    ClientContext context;
    HeartbeatMessage message;
    message.set_cluster_id(SYNC_CLUSTER_NUM);
    message.set_server_id(server_id);
    Empty response;
    auto status = coordinator_stub->Heartbeat(&context, message, &response);
    if (!status.ok()) {
      log(ERROR, "Heartbeat failed");
      break;
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

void run_synchronizer(const std::string &coordIP, uint32_t coordPort,
                      uint32_t port, int synchID,
                      SynchronizerRabbitMQ &rabbitMQ) {
  // setup coordinator stub
  std::string target_str = coordIP + ":" + std::to_string(coordPort);
  auto coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));

  // register as a synchronizer with the coordinator
  ClientContext context;
  ServerRegistration registration;
  registration.set_cluster_id(SYNC_CLUSTER_NUM);
  registration.add_capabilities(ServerCapability::SNS_SYNCHRONIZER);
  ServerInfo *serverInfo = registration.mutable_info();
  serverInfo->set_id(synchID);
  serverInfo->set_hostname("localhost");
  serverInfo->set_port(port);

  RegistrationResponse response;

  auto status = coord_stub_->Register(&context, registration, &response);

  if (status.ok()) {
    log(INFO, "Registered with coordinator");
    std::thread heartbeat_thread(Heartbeat, std::move(coord_stub_), synchID);
    heartbeat_thread.detach();
  } else {
    log(ERROR, "Failed to register with coordinator");
  }

  // TODO: begin synchronization process
  while (true) {
    // the synchronizers sync files every 5 seconds
    sleep(5);

    grpc::ClientContext context;
    // ServerList followerServers;
    // ID id;
    // id.set_id(synchID);

    // making a request to the coordinator to see count of follower
    // synchronizers
    // coord_stub_->GetAllFollowerServers(&context, id, &followerServers);

    std::vector<int> server_ids;
    std::vector<std::string> hosts, ports;
    // for (std::string host : followerServers.hostname()) {
    //   hosts.push_back(host);
    // }
    // for (std::string port : followerServers.port()) {
    //   ports.push_back(port);
    // }
    // for (int serverid : followerServers.serverid()) {
    //   server_ids.push_back(serverid);
    // }

    // update the count of how many follower synchronizer processes the
    // coordinator has registered

    // below here, you run all the update functions that synchronize the state
    // across all the clusters make any modifications as necessary to satisfy
    // the assignments requirements

    // Publish user list
    rabbitMQ.publishUserList();

    // Publish client relations
    rabbitMQ.publishClientRelations();

    // Publish timelines
    rabbitMQ.publishTimelines();
  }
  return;
}

void RunServer(const std::string &server_folder,
               const std::string &coordinator_address, int synchID,
               int port_no) {
  std::string server_address("127.0.0.1:" + std::to_string(port_no));
  log(INFO, "Starting synchronizer server at " + server_address);
  // SynchServiceImpl service;
  // grpc::EnableDefaultHealthCheckService(true);
  // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  // builder.AddListeningPort(server_address,
  // grpc::InsecureServerCredentials()); Register "service" as the instance
  // through which we'll communicate with clients. In this case it corresponds
  // to an *synchronous* service. builder.RegisterService(&service); Finally
  // assemble the server. std::unique_ptr<Server>
  // server(builder.BuildAndStart()); std::cout << "Server listening on " <<
  // server_address << std::endl;

  // Initialize RabbitMQ connection
  SynchronizerRabbitMQ rabbitMQ("localhost", 5672, synchID);

  std::thread t1(run_synchronizer, port_no, synchID, std::ref(rabbitMQ));

  // Create a consumer thread
  std::thread consumerThread([&rabbitMQ]() {
    while (true) {
      rabbitMQ.consumeUserLists();
      rabbitMQ.consumeClientRelations();
      rabbitMQ.consumeTimelines();
      std::this_thread::sleep_for(std::chrono::seconds(5));
      // you can modify this sleep period as per your choice
    }
  });

  server->Wait();

  //   t1.join();
  //   consumerThread.join();
}

int main(int argc, char **argv) {
  std::string coordinator_ip = "localhost";
  uint32_t coordinator_port = 9090;
  uint32_t port = 3029;
  uint32_t server_id = 1;

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1) {
    switch (opt) {
      case 'h':
        coordinator_ip = optarg;
        break;
      case 'k':
        coordinator_port = atoi(optarg);
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'i':
        server_id = atoi(optarg);
        break;
      default:
        std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_name = std::string("synchronizer-") + std::to_string(port);
  google::InitGoogleLogging(log_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");

  // derive cluster id from server id
  int cluster_id = (server_id - 1) / 3 + 1;
  std::string server_folder = "server_" + std::to_string(cluster_id) + "_" +
                              std::to_string(server_id) + "/";
  std::string coordinator_address =
      coordinator_ip + ":" + std::to_string(coordinator_port);

  RunServer(server_folder, coordinator_address, server_id, port);
  return 0;
}