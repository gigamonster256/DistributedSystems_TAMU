#include <errno.h>
#include <glog/logging.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <thread>

#include "coordinator.grpc.pb.h"
#include "proto_helpers.h"
#include "sns.grpc.pb.h"
#include "sns_db.h"

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

using namespace csce662;

using google::protobuf::Empty;
using google::protobuf::Timestamp;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using grpc::ClientContext;

typedef ServerReaderWriter<Message, Message> TimelineStream;

typedef std::vector<std::unique_ptr<SNSService::Stub>> SlaveStubList;

SlaveStubList slaveStubs;

class SNSServiceImpl final : public SNSService::Service {
 private:
  SNSDatabase user_db;
  std::set<std::string> logged_in_users;
  std::unordered_map<std::string, TimelineStream*> timeline_streams;

  // service methods
  Status Login(ServerContext*, const LoginRequest* request,
               Empty* response) override;
  Status Logout(ServerContext*, const LogoutRequest* request,
                Empty* response) override;
  Status List(ServerContext*, const ListRequest* request,
              ListReply* list_reply) override;
  Status Follow(ServerContext*, const FollowRequest* request, Empty*) override;
  Status UnFollow(ServerContext*, const FollowRequest* request,
                  Empty*) override;
  Status Timeline(ServerContext* context, TimelineStream* stream) override;
  Status Ping(ServerContext*, const Empty* request, Empty* response) override;
  Status Post(ServerContext*, const Message* request, Empty* response) override;

 public:
  SNSServiceImpl(const std::string& server_folder) : user_db(server_folder) {}
};

class SNSReplicatorServiceImpl final : public ReplicatorService::Service {
  Status InformNewSlave(ServerContext*, const ServerInfo* request,
                        Empty* response) override;
};

Status SNSReplicatorServiceImpl::InformNewSlave(ServerContext*,
                                                const ServerInfo* request,
                                                Empty*) {
  auto [hostname, port] = get_server_address(*request);
  auto server_address = hostname + ":" + std::to_string(port);
  log(INFO, "InformNewSlave: " + server_address);

  // create a stub for the new slave
  auto stub = SNSService::NewStub(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
  slaveStubs.push_back(std::move(stub));
  return Status::OK;
}

Status SNSServiceImpl::List(ServerContext*, const ListRequest* request,
                            ListReply* list_reply) {
  auto username = request->username();
  auto user_opt = user_db[username];
  // user not found
  if (user_opt == std::nullopt) {
    log(ERROR, "List: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
  }
  auto& user = user_opt.value();
  log(INFO, "List request from " + user.get_username());
  auto all_users = user_db.get_all_usernames();
  for (auto& user : all_users) {
    list_reply->add_all_users(user);
  }
  auto following = user.get_following();
  for (auto& username : following) {
    list_reply->add_followers(username);
  }
  return Status::OK;
}

Status SNSServiceImpl::Follow(ServerContext*, const FollowRequest* request,
                              Empty*) {
  const auto& username = request->username();
  auto following_user_opt = user_db[username];
  if (following_user_opt == std::nullopt) {
    log(ERROR, "Follow: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
  }
  auto following_user = following_user_opt.value();
  log(INFO, "Follow request from " + following_user.get_username());

  const auto& being_followed_username = request->follower();

  log(INFO, "User " + username + " following " + being_followed_username);

  // make sure not following self
  if (username == being_followed_username) {
    log(ERROR, "Follow: Cannot follow self");
    return Status(grpc::StatusCode::INVALID_ARGUMENT, "Cannot follow self");
  }

  // make sure not already following
  for (auto& already_followed_username : following_user.get_following()) {
    if (already_followed_username == being_followed_username) {
      log(ERROR, "Follow: Already following");
      return Status(grpc::StatusCode::ALREADY_EXISTS, "Already following");
    }
  }

  // get the client that is being followed
  const auto being_followed_user_opt = user_db[being_followed_username];
  if (being_followed_user_opt == std::nullopt) {
    log(ERROR, "Follow: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "Following user not found");
  }

  following_user.add_follower(being_followed_username);

  // replicate to slaves
  for (auto& stub : slaveStubs) {
    Empty response;
    ClientContext context;
    auto status = stub->Follow(&context, *request, &response);
    if (!status.ok()) {
      log(ERROR, "Follow: Slave failed to replicate");
    }
  }

  return Status::OK;
}

Status SNSServiceImpl::UnFollow(ServerContext*, const FollowRequest* request,
                                Empty*) {
  const auto& username = request->username();
  auto unfollowing_user_opt = user_db[username];
  if (unfollowing_user_opt == std::nullopt) {
    log(ERROR, "UnFollow: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
  }
  auto unfollowing_user = unfollowing_user_opt.value();
  log(INFO, "Unfollow request from " + unfollowing_user.get_username());

  // get the user that this user wants to unfollow
  const auto& being_unfollowed_username = request->follower();

  // make sure not unfollowing self
  if (username == being_unfollowed_username) {
    log(ERROR, "UnFollow: Cannot unfollow self");
    return Status(grpc::StatusCode::INVALID_ARGUMENT, "Cannot unfollow self");
  }

  // make sure already following
  bool found = false;
  for (auto& username : unfollowing_user.get_following()) {
    if (username == being_unfollowed_username) {
      found = true;
      break;
    }
  }
  if (!found) {
    log(ERROR, "UnFollow: Not following");
    return Status(grpc::StatusCode::NOT_FOUND, "Not following");
  }

  // get the client that is being unfollowed
  const auto being_unfollowed_user_opt = user_db[being_unfollowed_username];
  if (being_unfollowed_user_opt == std::nullopt) {
    log(ERROR, "UnFollow: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "Following user not found");
  }

  unfollowing_user.remove_follower(being_unfollowed_username);

  // replicate to slaves
  for (auto& stub : slaveStubs) {
    Empty response;
    ClientContext context;
    auto status = stub->UnFollow(&context, *request, &response);
    if (!status.ok()) {
      log(ERROR, "UnFollow: Slave failed to replicate");
    }
  }

  return Status::OK;
}

Status SNSServiceImpl::Login(ServerContext*, const LoginRequest* request,
                             Empty*) {
  const std::string& username = request->username();
  log(INFO, "Login request from " + username);

  // check if logged in already
  if (logged_in_users.find(username) != logged_in_users.end()) {
    log(ERROR, "Login: User already logged in");
    return Status(grpc::StatusCode::ALREADY_EXISTS, "User already logged in");
  }

  // get/create user
  user_db.create_user_if_not_exists(username);
  logged_in_users.insert(username);

  // call login on slave servers
  for (auto& stub : slaveStubs) {
    Empty response;
    ClientContext context;
    auto status = stub->Login(&context, *request, &response);
    if (!status.ok()) {
      log(ERROR, "Login: Slave failed to replicate");
    }
  }

  return Status::OK;
}

Status SNSServiceImpl::Logout(ServerContext*, const LogoutRequest* request,
                              Empty*) {
  const std::string& username = request->username();
  log(INFO, "Logout request from " + username);

  // check if user is logged in
  if (logged_in_users.find(username) == logged_in_users.end()) {
    log(ERROR, "Logout: User not logged in");
    return Status(grpc::StatusCode::NOT_FOUND, "User not logged in");
  }

  // set logged out
  logged_in_users.erase(username);

  if (auto stream = timeline_streams[username]) {
    // send last message to stream
    stream->WriteLast(Message(), grpc::WriteOptions());

    // clear stream
    timeline_streams.erase(username);
  }

  // call logout on slave servers
  for (auto& stub : slaveStubs) {
    Empty response;
    ClientContext context;
    auto status = stub->Logout(&context, *request, &response);
    if (!status.ok()) {
      log(ERROR, "Logout: Slave failed to replicate");
    }
  }

  return Status::OK;
}

Status SNSServiceImpl::Timeline(ServerContext* context,
                                TimelineStream* stream) {
  // get session token from client metadata
  const auto& metadata = context->client_metadata();
  std::string username;
  for (auto it = metadata.begin(); it != metadata.end(); it++) {
    if (it->first == "username") {
      username = std::string(it->second.data(), it->second.length());
      break;
    }
  }
  if (username.empty()) {
    log(ERROR, "Timeline: Missing username");
    return Status(grpc::StatusCode::FAILED_PRECONDITION, "Missing username");
  }

  // save the stream so that who we are following can send us messages
  auto user_opt = user_db[username];

  if (user_opt == std::nullopt) {
    log(ERROR, "Timeline: Invalid user");
    return Status(grpc::StatusCode::FAILED_PRECONDITION, "Invalid user");
  }

  auto user = user_opt.value();

  log(INFO, "Timeline request from " + user.get_username());

  // save the stream
  timeline_streams[username] = stream;

  auto timeline = user.get_timeline();

  // send the timeline to the client reverse order
  for (auto it = timeline.rbegin(); it != timeline.rend(); it++) {
    stream->Write(*it);
  }

  // forward messages to followers
  Message message;
  while (stream->Read(&message)) {
    // make sure the user is sending messages as themselves
    const std::string& username = message.username();
    if (username != user.get_username()) {
      log(ERROR, "Timeline: User " + user.get_username() +
                     " trying to send message as " + username);
      stream->WriteLast(Message(), grpc::WriteOptions());
      return Status(grpc::StatusCode::PERMISSION_DENIED,
                    "Cannot send message as another user");
    }
    log(INFO, "Message from " + username + " to followers");
    // save to self file
    user.post(message);

    // forward to slave servers
    for (auto& stub : slaveStubs) {
      Empty response;
      ClientContext context;
      auto status = stub->Post(&context, message, &response);
      if (!status.ok()) {
        log(ERROR, "Timeline: Slave failed to replicate");
      }
    }

    // forward to followers if they are logged in
    for (auto& follower : user.get_followers()) {
      if (auto stream = timeline_streams[follower]) {
        stream->Write(message);
      }
    }
  }

  return Status::OK;
}

Status SNSServiceImpl::Ping(ServerContext*, const Empty*, Empty*) {
  return Status::OK;
}

Status SNSServiceImpl::Post(ServerContext*, const Message* request, Empty*) {
  const std::string& username = request->username();
  log(INFO, "Post request from " + username);

  auto user_opt = user_db[username];
  if (user_opt == std::nullopt) {
    log(ERROR, "Post: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
  }

  auto user = user_opt.value();
  user.post(*request);

  return Status::OK;
}

void Heartbeat(std::unique_ptr<CoordService::Stub> coordinator_stub,
               const uint32_t cluster_id, const uint32_t server_id) {
  while (true) {
    ClientContext context;
    HeartbeatMessage message;
    message.set_cluster_id(cluster_id);
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

void RegisterAndHeartbeat(const std::string& coordinator_address,
                          const uint32_t cluster_id, const uint32_t server_id,
                          const std::string& server_address,
                          const uint32_t port) {
  auto coordinator_stub = CoordService::NewStub(grpc::CreateChannel(
      coordinator_address, grpc::InsecureChannelCredentials()));

  ClientContext context;

  ServerRegistration registration;
  registration.set_cluster_id(cluster_id);
  // we do SNS and SNS_REPLICATOR
  registration.add_capabilities(ServerCapability::SNS);
  registration.add_capabilities(ServerCapability::SNS_REPLICATOR);
  ServerInfo* server_info = registration.mutable_info();
  server_info->set_id(server_id);
  server_info->set_hostname(server_address);
  server_info->set_port(port);

  RegistrationResponse response;

  auto status = coordinator_stub->Register(&context, registration, &response);

  if (status.ok()) {
    log(INFO, "Registered with coordinator");
    std::thread heartbeat_thread(Heartbeat, std::move(coordinator_stub),
                                 cluster_id, server_id);
    heartbeat_thread.detach();
  } else {
    log(ERROR, "Failed to register with coordinator");
  }
}

void RunServer(const std::string& server_folder,
               const std::string& coordinator_address,
               const uint32_t cluster_id, const uint32_t server_id,
               const uint32_t port) {
  const std::string server_address = "127.0.0.1";
  SNSServiceImpl service(server_folder);
  SNSReplicatorServiceImpl replicator;

  ServerBuilder builder;
  builder.AddListeningPort(server_address + ":" + std::to_string(port),
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  builder.RegisterService(&replicator);
  auto server = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on " + server_address);

  RegisterAndHeartbeat(coordinator_address, cluster_id, server_id,
                       server_address, port);

  server->Wait();
}

int main(int argc, char** argv) {
  uint32_t cluster_id = 1;
  uint32_t server_id = 1;
  std::string coordinator_ip = "localhost";
  uint32_t coordinator_port = 9090;
  uint32_t port = 3010;

  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1) {
    switch (opt) {
      case 'c':
        cluster_id = std::stoi(optarg);
        break;
      case 's':
        server_id = std::stoi(optarg);
        break;
      case 'h':
        coordinator_ip = optarg;
        break;
      case 'k':
        coordinator_port = std::stoi(optarg);
        break;
      case 'p':
        port = std::stoi(optarg);
        break;
      default:
        std::cerr << "Invalid Command Line Argument" << std::endl;
    }
  }

  // create the server folder
  std::string server_folder = "server_" + std::to_string(cluster_id) + "_" +
                              std::to_string(server_id) + "/";
  if (mkdir(server_folder.c_str(), 0777) == -1) {
    if (errno != EEXIST) {
      log(ERROR, "Failed to create server folder");
      perror("mkdir");
      return 1;
    }
  }

  // initialize logging
  std::string log_name = "sns_server_" + std::to_string(cluster_id) + "_" +
                         std::to_string(server_id) + ".log";
  google::InitGoogleLogging(log_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");

  std::string coordinator_address =
      coordinator_ip + ":" + std::to_string(coordinator_port);
  RunServer(server_folder, coordinator_address, cluster_id, server_id, port);

  return 0;
}