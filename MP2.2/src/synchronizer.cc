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
#include "sns_db.h"

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

bool is_master = false;

class syncronizerRabbitMQ {
 private:
  // runtime data
  amqp_connection_state_t conn;
  amqp_channel_t channel;
  SNSDatabase db;

  // configuration
  std::string hostname;
  int port;
  int sync_id;
  int cluster_id;
  int server_id;

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
  syncronizerRabbitMQ(const std::string &server_folder, int cluster_id,
                      int server_id, int id)
      : channel(1),
        db(server_folder),
        hostname("localhost"),
        port(5672),
        sync_id(id),
        cluster_id(cluster_id),
        server_id(server_id) {
    setupRabbitMQ();
    declareQueue("sync" + std::to_string(id) + "_users_queue");
    declareQueue("sync" + std::to_string(id) + "_clients_relations_queue");
    declareQueue("sync" + std::to_string(id) + "_timeline_queue");
  }

  void publishUserList() {
    auto users = db.get_all_usernames();
    if (users.empty()) {
      return;
    }
    Json::Value userList;
    for (const auto &user : users) {
      userList["users"].append(user);
    }
    Json::FastWriter writer;
    std::string message = writer.write(userList);
    publishMessage("sync" + std::to_string(cluster_id) + "_users_queue",
                   message);
  }

  void consumeUserLists() {
    std::set<std::string> allUsers;

    // read the _users_queue from all other cluster_ids
    for (auto id : {1, 2, 3}) {
      if (id == cluster_id) {
        continue;
      }
      std::string queueName = "sync" + std::to_string(id) + "_users_queue";
      std::string message = consumeMessage(queueName, 0);  // no timeout
      if (message.empty()) {
        continue;
      }
      Json::Reader reader;
      Json::Value users;
      reader.parse(message, users);
      for (const auto &user : users["users"]) {
        allUsers.insert(user.asString());
      }
    }
    // endure all users exist in the DB
    for (const auto &user : allUsers) {
      db.create_user_if_not_exists(user);
    }
  }

  void publishUserRelations() {
    Json::Value relations;
    auto usernames = db.get_all_usernames();

    for (const auto &username : usernames) {
      auto user = db[username].value();
      auto followers = user.get_followers();

      Json::Value followerList(Json::arrayValue);
      for (const auto &follower : followers) {
        followerList.append(follower);
      }

      if (!followerList.empty()) {
        relations[username] = followerList;
      }
    }

    Json::FastWriter writer;
    std::string message = writer.write(relations);
    publishMessage(
        "sync" + std::to_string(sync_id) + "_clients_relations_queue", message);
  }

  void consumeClientRelations() {
    auto usernames = db.get_all_usernames();

    // YOUR CODE HERE

    auto thingy = std::to_string(cluster_id) + std::to_string(server_id);

    (void)(thingy);

    // TODO: hardcoding 6 here, but you need to get list of all syncronizers
    // from coordinator as before
    for (int i = 1; i <= 6; i++) {
      std::string queueName =
          "sync" + std::to_string(i) + "_clients_relations_queue";
      std::string message =
          consumeMessage(queueName, 1000);  // 1 second timeout
    }
  }

  // for every client in your cluster, update all their followers' timeline
  // files by publishing your user's timeline file (or just the new updates in
  // them)
  //  periodically to the message queue of the syncronizer responsible for that
  //  client
  void publishTimelines() {
    auto usernames = db.get_all_usernames();

    // for (const auto &_ : usernames) {
    // get the client ID from the username (e.g. u1 -> 1)
    // int client_id = std::stoi(username.substr(1));
    // int client_cluster = ((client_id - 1) % 3) + 1;
    // only do this for clients in your own cluster
    // if (client_cluster != clusterID) {
    //   continue;
    // }

    // std::vector<std::string> timeline = get_tl_or_fl(sync_id, clientId,
    // true); std::vector<std::string> followers =
    // getFollowersOfUser(clientId);

    // for (const auto &follower : followers) {
    //   // send the timeline updates of your current user to all its
    //   followers

    //   // YOUR CODE HERE
    // }
    // }
  }

  // For each client in your cluster, consume messages from your timeline queue
  // and modify your client's timeline files based on what the users they follow
  // posted to their timeline
  void consumeTimelines() {
    std::string queueName =
        "sync" + std::to_string(sync_id) + "_timeline_queue";
    std::string message = consumeMessage(queueName, 1000);  // 1 second timeout

    if (!message.empty()) {
      // consume the message from the queue and update the timeline file of the
      // appropriate client with the new updates to the timeline of the user it
      // follows

      // YOUR CODE HERE
    }
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

void run_syncronizer(const std::string &coord_address, int port, int sync_id,
                     syncronizerRabbitMQ &rabbitMQ) {
  // setup coordinator stub
  auto coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(
      grpc::CreateChannel(coord_address, grpc::InsecureChannelCredentials())));

  // register as a syncronizer with the coordinator
  ClientContext context;
  ServerRegistration registration;
  registration.set_cluster_id(SYNC_CLUSTER_NUM);
  registration.add_capabilities(ServerCapability::SNS_SYNCHRONIZER);
  ServerInfo *serverInfo = registration.mutable_info();
  serverInfo->set_id(sync_id);
  serverInfo->set_hostname("127.0.0.1");
  serverInfo->set_port(port);

  RegistrationResponse response;

  auto status = coord_stub_->Register(&context, registration, &response);

  if (status.ok()) {
    log(INFO, "Registered with coordinator");
    std::thread heartbeat_thread(Heartbeat, std::move(coord_stub_), sync_id);
    heartbeat_thread.detach();
  } else {
    log(ERROR, "Failed to register with coordinator");
    exit(1);
  }

  is_master = response.status() == ClusterStatus::MASTER;
  if (response.status() == ClusterStatus::MASTER) {
    log(INFO, "I am the master syncronizer");
  } else {
    log(INFO, "I am a slave syncronizer");
  }

  // TODO: begin synchronization process
  while (true) {
    // the syncronizers sync files every 5 seconds
    sleep(5);

    if (!is_master) {
      continue;
    }

    // grpc::ClientContext context;
    // ServerList followerServers;
    // ID id;
    // id.set_id(sync_id);

    // making a request to the coordinator to see count of follower
    // syncronizers
    // coord_stub_->GetAllFollowerServers(&context, id, &followerServers);

    // std::vector<int> server_ids;
    // std::vector<std::string> hosts, ports;
    // for (std::string host : followerServers.hostname()) {
    //   hosts.push_back(host);
    // }
    // for (std::string port : followerServers.port()) {
    //   ports.push_back(port);
    // }
    // for (int serverid : followerServers.serverid()) {
    //   server_ids.push_back(serverid);
    // }

    // update the count of how many follower syncronizer processes the
    // coordinator has registered

    // below here, you run all the update functions that synchronize the state
    // across all the clusters make any modifications as necessary to satisfy
    // the assignments requirements

    // Publish user list
    rabbitMQ.publishUserList();

    // Publish client relations
    // rabbitMQ.publishUserRelations();

    // // Publish timelines
    // rabbitMQ.publishTimelines();
  }
  return;
}

class SyncServiceImpl final : public SynchronizerService::Service {
  Status SetStatus(ServerContext *, const RegistrationResponse *status,
                   Empty *) override {
    is_master = status->status() == MASTER;
    if (is_master) {
      log(INFO, "I am the master syncronizer now");
    } else {
      log(INFO, "I am the slave now");
    }
    return Status::OK;
  }
};

void RunServer(const std::string &server_folder,
               const std::string &coordinator_address, int cluster_id,
               int server_id, int sync_id, int port_no) {
  auto server_address = "127.0.0.1:" + std::to_string(port_no);
  log(INFO, "Starting syncronizer server at " + server_address);
  SyncServiceImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server(builder.BuildAndStart());

  // Initialize RabbitMQ connection
  syncronizerRabbitMQ rabbitMQ(server_folder, cluster_id, server_id, sync_id);

  std::thread t1(run_syncronizer, coordinator_address, port_no, sync_id,
                 std::ref(rabbitMQ));

  // Create a consumer thread
  std::thread consumerThread([&rabbitMQ]() {
    while (true) {
      rabbitMQ.consumeUserLists();
      // rabbitMQ.consumeClientRelations();
      // rabbitMQ.consumeTimelines();
      std::this_thread::sleep_for(std::chrono::seconds(5));
      // you can modify this sleep period as per your choice
    }
  });

  log(INFO, "Server listening on " + server_address);

  server->Wait();

  t1.join();
  consumerThread.join();
}

int main(int argc, char **argv) {
  std::string coordinator_ip = "localhost";
  uint32_t coordinator_port = 9090;
  uint32_t port = 3029;
  uint32_t sync_id = 1;

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
        sync_id = atoi(optarg);
        break;
      default:
        std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_name = std::string("syncronizer-") + std::to_string(port);
  google::InitGoogleLogging(log_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");

  // derive cluster id from server id
  int cluster_id = (sync_id + 1) / 2;
  int server_id = (sync_id - 1) % 2 + 1;
  std::string server_folder = "server_" + std::to_string(cluster_id) + "_" +
                              std::to_string(server_id) + "/";
  std::cerr << "Server folder: " << server_folder << std::endl;
  std::string coordinator_address =
      coordinator_ip + ":" + std::to_string(coordinator_port);

  RunServer(server_folder, coordinator_address, cluster_id, server_id, sync_id,
            port);
  return 0;
}