#include <grpc++/grpc++.h>
#include <unistd.h>

#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client.h"
#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"

using namespace csce662;

using google::protobuf::Empty;
using google::protobuf::Timestamp;

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;

Message MakeMessage(const std::string& username, const std::string& msg) {
  Message m;
  m.set_username(username);
  m.set_msg(msg);
  auto* timestamp = new Timestamp();
  timestamp->set_seconds(time(NULL));
  timestamp->set_nanos(0);
  m.set_allocated_timestamp(timestamp);
  return m;
}

SNSStatus convertGRPCStatusToSNSStatus(const Status& status) {
  if (status.ok()) {
    return SUCCESS;
  } else if (status.error_code() == grpc::StatusCode::ALREADY_EXISTS) {
    return FAILURE_ALREADY_EXISTS;
  } else if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
    return FAILURE_NOT_EXISTS;
  } else {
    return FAILURE_UNKNOWN;
  }
}

class Client : public IClient {
 public:
  Client(const uint32_t user_id, const std::string& hostname, uint32_t port)
      : username(std::string("u") + std::to_string(user_id)),
        hostname(hostname),
        port(port),
        user_id(user_id) {}

 protected:
  virtual SNSStatus connect();
  virtual SNSReply processCommand(SNSCommand);
  virtual void processTimeline();

 private:
  // config data
  std::string username;
  std::string hostname;
  uint32_t port;
  uint32_t user_id;

  SNSStatus Login();
  SNSReply List();
  SNSStatus Follow(const std::string& username);
  SNSStatus UnFollow(const std::string& username);
  std::pair<std::string, uint32_t> getHostnameAndPort() const;
  SNSStatus Timeline();
};

std::shared_ptr<grpc::Channel> channel_;
std::unique_ptr<SNSService::Stub> stub_;

void reconnectChannel() { channel_->GetState(true); }

SNSStatus Client::connect() {
  auto hostname_and_port = getHostnameAndPort();
  std::string& hostname = hostname_and_port.first;
  uint32_t& port = hostname_and_port.second;
  channel_ = grpc::CreateChannel(hostname + ":" + std::to_string(port),
                                 grpc::InsecureChannelCredentials());
  stub_ = SNSService::NewStub(channel_);
  return Login();
}

SNSReply Client::processCommand(SNSCommand cmd) {
  SNSReply reply;
  switch (cmd.first) {
    case LIST:
      reply = List();
      break;
    case FOLLOW:
      assert(cmd.second.has_value());
      reply.first = Follow(cmd.second.value());
      break;
    case UNFOLLOW:
      assert(cmd.second.has_value());
      reply.first = UnFollow(cmd.second.value());
      break;
    case TIMELINE:
      reply.first = Timeline();
      break;
    default:
      reply.first = FAILURE_INVALID;
      break;
  }
  return reply;
}

void Client::processTimeline() { while (true); }

// List Command
SNSReply Client::List() {
  reconnectChannel();
  SNSReply reply;

  ClientContext context;

  ListRequest request;
  request.set_username(username);

  ListReply list_reply;

  auto status = stub_->List(&context, request, &list_reply);

  reply.first = convertGRPCStatusToSNSStatus(status);
  reply.second = std::make_unique<SNSListReply>(
      std::vector<std::string>(list_reply.all_users().begin(),
                               list_reply.all_users().end()),
      std::vector<std::string>(list_reply.followers().begin(),
                               list_reply.followers().end()));
  return reply;
}

SNSStatus Client::Follow(const std::string& username2) {
  reconnectChannel();
  ClientContext context;

  FollowRequest request;
  request.set_username(username);
  request.set_follower(username2);

  Empty reply;

  auto status = stub_->Follow(&context, request, &reply);

  return convertGRPCStatusToSNSStatus(status);
}

// UNFollow Command
SNSStatus Client::UnFollow(const std::string& username2) {
  reconnectChannel();
  ClientContext context;

  FollowRequest request;
  request.set_username(username);
  request.set_follower(username2);

  Empty reply;

  auto status = stub_->UnFollow(&context, request, &reply);

  return convertGRPCStatusToSNSStatus(status);
}

// Login Command
SNSStatus Client::Login() {
  ClientContext context;

  LoginRequest request;
  request.set_id(0);
  request.set_username(username);

  Empty reply;

  auto status = stub_->Login(&context, request, &reply);

  return convertGRPCStatusToSNSStatus(status);
}

SNSStatus Client::Timeline() {
  reconnectChannel();
  ClientContext context;
  context.AddMetadata("username", username);

  // check channel
  if (channel_->GetState(true) != grpc_connectivity_state::GRPC_CHANNEL_READY) {
    return FAILURE_UNKNOWN;
  }

  std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
      stub_->Timeline(&context));

  if (!stream) {
    std::cerr << "Failed to write to stream" << std::endl;
    return FAILURE_UNKNOWN;
  }

  std::cout << "Now you are in the timeline" << std::endl;

  std::thread writer([this, stream]() {
    // while stream is open
    while (stream->Write(MakeMessage(this->username, getPostMessage())));
  });

  Message m;
  while (stream->Read(&m)) {
    time_t t = m.timestamp().seconds();
    displayPostMessage(m.username(), m.msg(), t);
  }
  // should never get here (no way to exit timeline mode)
  writer.join();
  return FAILURE_UNKNOWN;
}

std::pair<std::string, uint32_t> Client::getHostnameAndPort() const {
  auto stub = CoordService::NewStub(
      grpc::CreateChannel(hostname + ":" + std::to_string(port),
                          grpc::InsecureChannelCredentials()));

  ClientContext context;
  ServerInfo server_info;
  ClientID client_id;
  client_id.set_id(user_id);

  Status status = stub->GetServer(&context, client_id, &server_info);

  if (!status.ok()) {
    std::cerr << "Failed to get server info from coordinator" << std::endl;
    exit(1);
  }

  return std::make_pair(server_info.hostname(), server_info.port());
}

int main(int argc, char** argv) {
  std::string hostname = "localhost";
  uint32_t port = 9090;
  uint32_t user_id = 1;

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:k:u:")) != -1) {
    switch (opt) {
      case 'h':
        hostname = optarg;
        break;
      case 'k':
        port = std::stoi(optarg);
        break;
      case 'u':
        user_id = std::stoi(optarg);
        break;
      default:
        std::cout << "Invalid Command Line Argument" << std::endl;
    }
  }

  std::cout << "Logging Initialized. Client starting...";

  Client myc(user_id, hostname, port);

  myc.run();

  return 0;
}
