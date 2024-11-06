#include <grpc++/grpc++.h>

#include <thread>

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

SNSStatus convertGRPCStatusToSNSStatus(const Status& status) {
  if (status.ok()) {
    return SUCCESS;
  }
  switch (status.error_code()) {
    case grpc::StatusCode::ALREADY_EXISTS:
      return FAILURE_ALREADY_EXISTS;
    case grpc::StatusCode::NOT_FOUND:
      return FAILURE_NOT_EXISTS;
    default:
      return FAILURE_UNKNOWN;
  }
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
      return std::make_pair(Timeline(), std::nullopt);
    default:
      return std::make_pair(FAILURE_INVALID, std::nullopt);
  }
}

void Client::processTimeline() {
  while (true)
    ;
}

// List Command
SNSReply Client::List() {
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
  request.set_username(username);

  Empty reply;

  auto status = stub_->Login(&context, request, &reply);

  return convertGRPCStatusToSNSStatus(status);
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

  std::cout << "Now you are in the timeline" << std::endl;

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

int main(int argc, char** argv) {
  std::string hostname = "localhost";
  uint32_t port = 9090;
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
