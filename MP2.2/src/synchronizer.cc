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
std::mutex rabbitMutex;

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
  // int server_id;

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
    std::lock_guard<std::mutex> lock(rabbitMutex);
    amqp_basic_publish(conn, channel, amqp_empty_bytes,
                       amqp_cstring_bytes(queueName.c_str()), 0, 0, NULL,
                       amqp_cstring_bytes(message.c_str()));
  }

  void publishToAllExceptSelfCluster(const std::string &message,
                                     const std::string &queuePostfix) {
    for (auto i : {1, 2, 3, 4, 5, 6}) {
      if (i == sync_id) {
        continue;
      }
      auto cluster_id = (i + 1) / 2;
      if (cluster_id == this->cluster_id) {
        continue;
      }
      publishMessage("sync" + std::to_string(i) + queuePostfix, message);
    }
  }

  std::string consumeMessage(const std::string &queueName,
                             int timeout_ms = 5000) {
    std::lock_guard<std::mutex> lock(rabbitMutex);
    amqp_basic_consume(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                       amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

    amqp_envelope_t envelope;

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
  syncronizerRabbitMQ(const std::string &server_folder, int cluster_id, int,
                      int id)
      : channel(1),
        db(server_folder),
        hostname("localhost"),
        port(5672),
        sync_id(id),
        cluster_id(cluster_id)
  // server_id(server_id)
  {
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
    // publish to all other syncronizers queues except self and same cluster
    publishToAllExceptSelfCluster(message, "_users_queue");
  }

  void consumeUserLists() {
    std::set<std::string> allUsers;

    std::string queueName = "sync" + std::to_string(sync_id) + "_users_queue";
    std::string message = consumeMessage(queueName, 0);  // no timeout
    if (message.empty()) {
      return;
    }
    Json::Reader reader;
    Json::Value users;
    reader.parse(message, users);
    processJson(users);
  }

  void processUserList(const Json::Value &users) {
    // std::cout << "Processing user list" << users << std::endl;
    for (const auto &user : users["users"]) {
      db.create_user_if_not_exists(user.asString());
    }
  }

  void publishUserRelations() {
    std::map<std::string, std::vector<std::string>> followers;

    for (auto &user : db.get_all_usernames()) {
      auto user_obj = db[user].value();
      followers[user] = user_obj.get_following();
    }

    Json::Value userRelations;
    for (const auto &[user, follower] : followers) {
      for (const auto &follower : follower) {
        userRelations["relations"][user].append(follower);
      }
    }

    if (userRelations.empty()) {
      return;
    }

    Json::FastWriter writer;
    std::string message = writer.write(userRelations);
    // std::cout << "Publishing user relations: " << message << std::endl;
    publishToAllExceptSelfCluster(message, "_clients_relations_queue");
  }

  void consumeClientRelations() {
    std::map<std::string, std::vector<std::string>> followers;

    // read the _clients_relations_queue from all other cluster_ids

    std::string queueName =
        "sync" + std::to_string(sync_id) + "_clients_relations_queue";
    std::string message = consumeMessage(queueName, 0);  // no timeout
    if (message.empty()) {
      return;
    }
    Json::Reader reader;
    Json::Value users;
    reader.parse(message, users);

    processJson(users);
  }

  void processUserRelations(const Json::Value &users) {
    // std::cout << "Processing user relations" << users << std::endl;
    for (const auto &user : users["relations"].getMemberNames()) {
      for (const auto &follower : users["relations"][user]) {
        db[user].value().add_follower(follower.asString());
      }
    }
  }

  // for every client in your cluster, update all their followers' timeline
  // files by publishing your user's timeline file (or just the new updates in
  // them)
  //  periodically to the message queue of the syncronizer responsible for that
  //  client
  void publishTimelines() {
    auto usernames = db.get_all_usernames();

    for (const auto &username : usernames) {
      auto user = db[username].value();
      auto timeline = user.get_timeline();
      if (timeline.empty()) {
        continue;
      }

      Json::Value timelineJson;
      for (const auto &message : timeline) {
        Json::Value messageJson;
        messageJson["username"] = message.username();
        messageJson["message"] = message.msg();
        messageJson["timestamp"] = message.timestamp().seconds();
        timelineJson["timeline"][username].append(messageJson);
      }

      Json::FastWriter writer;
      std::string message = writer.write(timelineJson);
      // std::cout << "Publishing timeline: " << message << std::endl;
      publishToAllExceptSelfCluster(message, "_timeline_queue");
    }
  }

  // For each client in your cluster, consume messages from your timeline queue
  // and modify your client's timeline files based on what the users they follow
  // posted to their timeline
  std::map<std::string, Message> lastMessage;
  void consumeTimelines() {
    // get a message from the timeline queues
    // std::cout << "Checking timeline queue: " << cluster_id << std::endl;
    std::string message =
        consumeMessage("sync" + std::to_string(sync_id) + "_timeline_queue", 0);
    if (message.empty()) {
      return;
    }

    Json::Reader reader;
    Json::Value timelines;
    reader.parse(message, timelines);

    processJson(timelines);
  }

  void processTimeline(const Json::Value &timelines) {
    // std::cout << "Processing timeline" << timelines << std::endl;
    for (const auto &user : timelines["timeline"].getMemberNames()) {
      // std::cout << "Consuming timeline: " << user << std::endl;
      std::vector<Message> timeline;
      for (const auto &message : timelines["timeline"][user]) {
        std::string username = message["username"].asString();
        std::string msg = message["message"].asString();
        int timestamp = message["timestamp"].asInt();

        Message msgObj;
        msgObj.set_username(username);
        msgObj.set_msg(msg);
        msgObj.mutable_timestamp()->set_seconds(timestamp);
        timeline.push_back(msgObj);
      }

      // print the timeline
      // for (const auto &msg : timeline) {
      //   std::cout << msg.username() << " " << msg.msg() << " "
      //             << msg.timestamp().seconds() << std::endl;
      // }

      // get the timeline saved to the database
      auto user_obj = db[user].value();
      auto dbTimeline = user_obj.get_timeline();

      // remove all the messages that are already in the database
      for (const auto &old_message : dbTimeline) {
        timeline.erase(
            std::remove_if(timeline.begin(), timeline.end(),
                           [&old_message](const Message &msg) {
                             return msg.msg() == old_message.msg() &&
                                    msg.username() == old_message.username() &&
                                    msg.timestamp().seconds() ==
                                        old_message.timestamp().seconds();
                           }),
            timeline.end());
      }

      // print new messages
      // for (const auto &msg : timeline) {
      //   std::cout << "New message: " << msg.username() << " " << msg.msg()
      //             << " " << msg.timestamp().seconds() << std::endl;
      // }

      // add the new messages to the database
      for (const auto &msg : timeline) {
        user_obj.add_message(msg);
      }
    }
  }

  // there seems to be a race condition when getting messages from named queues
  // because of this, we have to get a message, parse it, then see where it
  // should actually go
  void processJson(const Json::Value &json) {
    // check if it as a users, relations, or timeline key
    if (json.isMember("users")) {
      processUserList(json);
    } else if (json.isMember("relations")) {
      processUserRelations(json);
    } else if (json.isMember("timeline")) {
      processTimeline(json);
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
    std::this_thread::sleep_for(std::chrono::seconds(1));
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
    sleep(1);

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
    rabbitMQ.publishUserRelations();

    // // Publish timelines
    rabbitMQ.publishTimelines();
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
      // queues dont seem to be working... everything is in one queue
      // no matter what the queue name is
      // auto message = rabbit
      rabbitMQ.consumeUserLists();
      rabbitMQ.consumeClientRelations();
      rabbitMQ.consumeTimelines();

      std::this_thread::sleep_for(std::chrono::milliseconds(100));
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