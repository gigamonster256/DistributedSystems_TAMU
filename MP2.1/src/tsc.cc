#include <grpc++/grpc++.h>

#include <map>
#include <thread>
#include <variant>

#include "client.h"
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

typedef std::pair<std::string, SNSStatus> SNSReason;
typedef std::vector<SNSReason> SNSReasons;
typedef std::map<grpc::StatusCode, std::variant<SNSStatus, SNSReasons>>
    SNSStatusMapping;

SNSStatus convertGRPCStatusToSNSStatus(const Status& status,
                                       const SNSStatusMapping& mapping) {
  if (status.ok()) {
    return SUCCESS;
  }
  auto it = mapping.find(status.error_code());
  if (it == mapping.end()) {
    return FAILURE_UNKNOWN;
  }
  if (std::holds_alternative<SNSStatus>(it->second)) {
    return std::get<SNSStatus>(it->second);
  }
  auto& vec =
      std::get<std::vector<std::pair<std::string, SNSStatus>>>(it->second);
  for (const auto& [msg, sns] : vec) {
    if (status.error_message() == msg) {
      return sns;
    }
  }
  return FAILURE_UNKNOWN;
}

class Client : public IClient {
 public:
  Client(const std::string& username, const std::string& hostname,
         uint32_t port)
      : username(username), hostname(hostname), port(port) {}

 protected:
  virtual SNSStatus connect() override;
  virtual SNSReply processCommand(SNSCommand) override;
  virtual void processTimeline() override;

 private:
  // config data
  std::string username;
  std::string hostname;
  uint32_t port;

  // grpc methods
  SNSStatus Login();
  SNSReply List();
  SNSStatus Follow(const std::string& username);
  SNSStatus UnFollow(const std::string& username);
  SNSStatus Timeline();
  SNSStatus Ping();

  // internal data
  std::shared_ptr<SNSService::Stub> stub_;
};

SNSStatus Client::connect() {
  auto server_address = hostname + ":" + std::to_string(port);
  stub_ = SNSService::NewStub(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
  return Login();
}

SNSReply Client::processCommand(SNSCommand cmd) {
  switch (cmd.first) {
    case LIST:
      return List();
    case FOLLOW:
      assert(cmd.second.has_value());
      return std::make_pair(Follow(cmd.second.value()), std::nullopt);
    case UNFOLLOW:
      assert(cmd.second.has_value());
      return std::make_pair(UnFollow(cmd.second.value()), std::nullopt);
    case TIMELINE:
      return std::make_pair(Ping(), std::nullopt);
    default:
      return std::make_pair(FAILURE_INVALID, std::nullopt);
  }
}

void Client::processTimeline() { Timeline(); }

// List Command
SNSReply Client::List() {
  SNSReply reply;

  ClientContext context;

  ListRequest request;
  request.set_username(username);

  ListReply list_reply;

  auto status = stub_->List(&context, request, &list_reply);

  using grpc::StatusCode;
  const SNSStatusMapping mapping = {
      // user not found
      {StatusCode::NOT_FOUND, FAILURE_INVALID_USERNAME},
  };

  reply.first = convertGRPCStatusToSNSStatus(status, mapping);
  reply.second = std::make_unique<SNSListReply>(
      std::vector<std::string>(list_reply.all_users().begin(),
                               list_reply.all_users().end()),
      std::vector<std::string>(list_reply.followers().begin(),
                               list_reply.followers().end()));
  return reply;
}

SNSStatus Client::Follow(const std::string& username2) {
  ClientContext context;

  FollowRequest request;
  request.set_username(username);
  request.set_follower(username2);

  Empty reply;

  auto status = stub_->Follow(&context, request, &reply);

  using grpc::StatusCode;
  const SNSStatusMapping mapping = {
      {StatusCode::NOT_FOUND,
       SNSReasons{// requesting user not found
                  {"User not found", FAILURE_INVALID_USERNAME},
                  // user to follow not found
                  {"Following user not found", FAILURE_INVALID_USERNAME}}},
      // already following
      {StatusCode::ALREADY_EXISTS, FAILURE_ALREADY_EXISTS},
      // cannot follow self
      {StatusCode::INVALID_ARGUMENT, FAILURE_ALREADY_EXISTS},
  };

  return convertGRPCStatusToSNSStatus(status, mapping);
}

// UNFollow Command
SNSStatus Client::UnFollow(const std::string& username2) {
  ClientContext context;

  FollowRequest request;
  request.set_username(username);
  request.set_follower(username2);

  Empty reply;

  auto status = stub_->UnFollow(&context, request, &reply);

  using grpc::StatusCode;
  const SNSStatusMapping mapping = {
      {StatusCode::NOT_FOUND,
       SNSReasons{// requesting user not found
                  {"User not found", FAILURE_INVALID_USERNAME},
                  // user to follow not found
                  {"Following user not found", FAILURE_INVALID_USERNAME},
                  // not following
                  {"Not following", FAILURE_NOT_A_FOLLOWER}}},
      // already following
      {StatusCode::ALREADY_EXISTS, FAILURE_ALREADY_EXISTS},
      // cannot unfollow self
      {StatusCode::INVALID_ARGUMENT, FAILURE_INVALID_USERNAME},
  };

  return convertGRPCStatusToSNSStatus(status, mapping);
}

// Login Command
SNSStatus Client::Login() {
  ClientContext context;

  LoginRequest request;
  request.set_username(username);

  Empty reply;

  auto status = stub_->Login(&context, request, &reply);

  using grpc::StatusCode;
  const SNSStatusMapping mapping = {
      // user already logged in
      {StatusCode::ALREADY_EXISTS, FAILURE_ALREADY_EXISTS},
  };

  return convertGRPCStatusToSNSStatus(status, mapping);
}

SNSStatus Client::Timeline() {
  ClientContext context;
  context.AddMetadata("username", username);

  std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
      stub_->Timeline(&context));

  if (!stream) {
    std::cerr << "Failed to write to stream" << std::endl;
    return FAILURE_UNKNOWN;
  }

  std::thread writer([this, stream]() {
    // while stream is open
    while (stream->Write(MakeMessage(this->username, getPostMessage())))
      ;
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

SNSStatus Client::Ping() {
  ClientContext context;

  Empty request;
  Empty reply;

  auto status = stub_->Ping(&context, request, &reply);

  const SNSStatusMapping mapping = {};

  return convertGRPCStatusToSNSStatus(status, mapping);
}

int main(int argc, char** argv) {
  std::string hostname = "localhost";
  uint32_t port = 3010;
  std::string username = "u1";

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:p:u:")) != -1) {
    switch (opt) {
      case 'h':
        hostname = optarg;
        break;
      case 'p':
        port = std::stoi(optarg);
        break;
      case 'u':
        username = optarg;
        break;
      default:
        std::cout << "Invalid Command Line Argument" << std::endl;
    }
  }

  std::cout << "Logging Initialized. Client starting...";

  Client myc(username, hostname, port);

  myc.run();

  return 0;
}
