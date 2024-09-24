#include <grpc++/grpc++.h>
#include <unistd.h>

#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client.h"
#include "sns.grpc.pb.h"
using csce662::ListReply;
using csce662::Message;
using csce662::Reply;
using csce662::Request;
using csce662::SNSService;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

void sig_ignore(int sig) { std::cout << "Signal caught " << sig; }

Message MakeMessage(const std::string& username, const std::string& msg) {
  Message m;
  m.set_username(username);
  m.set_msg(msg);
  google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
  timestamp->set_seconds(time(NULL));
  timestamp->set_nanos(0);
  m.set_allocated_timestamp(timestamp);
  return m;
}

class Client : public IClient {
 public:
  Client(const std::string& hname, const std::string& uname,
         const std::string& p)
      : hostname(hname), username(uname), port(p) {}

 protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

 private:
  std::string hostname;
  std::string username;
  std::string port;
  std::string session_token;

  IReply Login();
  IReply List();
  IReply Follow(const std::string& username);
  IReply UnFollow(const std::string& username);
  void Timeline(const std::string& username);
};

std::unique_ptr<SNSService::Stub> stub_;

///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo() {
  // create stub
  auto channel = grpc::CreateChannel(hostname + ":" + port,
                                     grpc::InsecureChannelCredentials());
  stub_ = SNSService::NewStub(channel);

  IReply ire = Login();
  if (!ire.grpc_status.ok() || ire.comm_status != SUCCESS) {
    return -1;
  }

  return 0;
}

IReply Client::processCommand(std::string& input) {
  
  std::string cmd = input;
  // use same parsing method as getCommand to split string
  std::size_t index = input.find_first_of(" ");
  // FOLLOW and UNFOLLOW
  if (index != std::string::npos) {
    cmd = input.substr(0, index);
    std::string argument = input.substr(index + 1, (input.length() - index));
    if (cmd == "FOLLOW") {
      return Follow(argument);
    } else if (cmd == "UNFOLLOW") {
      return UnFollow(argument);
    }
    IReply ire;
    ire.comm_status = FAILURE_INVALID;
    return ire;
  }
  // LIST and TIMELINE
  if (cmd == "LIST") {
    return List();
  }
  if (cmd == "TIMELINE") {
    // client.run() will handle this in processTimeline
    // weird but fits the starter code
    return IReply();
  }
  IReply ire;
  ire.comm_status = FAILURE_INVALID;
  return ire;
}

void Client::processTimeline() { Timeline(username); }

// List Command
IReply Client::List() {
  IReply ire;

  ClientContext context;

  Request request;
  request.set_username(session_token);

  ListReply reply;

  ire.grpc_status = stub_->List(&context, request, &reply);

  if (ire.grpc_status.ok()) {
    ire.comm_status = SUCCESS;
    for (int i = 0; i < reply.all_users_size(); i++) {
      ire.all_users.push_back(reply.all_users(i));
    }
    for (int i = 0; i < reply.followers_size(); i++) {
      ire.followers.push_back(reply.followers(i));
    }
  } else {
    ire.comm_status = FAILURE_UNKNOWN;
  }

  return ire;
}

// Follow Command
IReply Client::Follow(const std::string& username2) {
  IReply ire;

  ClientContext context;

  Request request;
  request.set_username(session_token);
  request.add_arguments(username2);

  Reply reply;

  ire.grpc_status = stub_->Follow(&context, request, &reply);

  if (ire.grpc_status.ok()) {
    ire.comm_status = SUCCESS;
  } else if (ire.grpc_status.error_code() ==
             grpc::StatusCode::INVALID_ARGUMENT) {
    // this error could be a few different things but only following self is
    // tested
    ire.grpc_status = Status::OK;
    ire.comm_status = FAILURE_ALREADY_EXISTS;
  } else {
    ire.grpc_status = Status::OK;
    ire.comm_status = FAILURE_INVALID_USERNAME;
  }

  return ire;
}

// UNFollow Command
IReply Client::UnFollow(const std::string& username2) {
  IReply ire;

  ClientContext context;

  Request request;
  request.set_username(session_token);
  request.add_arguments(username2);

  Reply reply;

  ire.grpc_status = stub_->UnFollow(&context, request, &reply);

  if (ire.grpc_status.ok()) {
    ire.comm_status = SUCCESS;
  } else {
    // a bit weird but fits what client.cc is doing
    // to print error messages
    ire.grpc_status = Status::OK;
    ire.comm_status = FAILURE_INVALID_USERNAME;
  }

  return ire;
}

// Login Command
IReply Client::Login() {
  IReply ire;

  ClientContext context;

  Request request;
  request.set_username(username);

  Reply reply;

  ire.grpc_status = stub_->Login(&context, request, &reply);

  if (ire.grpc_status.ok()) {
    ire.comm_status = SUCCESS;
    session_token = reply.msg();
  } else if (ire.grpc_status.error_code() == grpc::StatusCode::ALREADY_EXISTS) {
    ire.comm_status = FAILURE_ALREADY_EXISTS;
  } else {
    ire.comm_status = FAILURE_UNKNOWN;
  }

  return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {
  // ------------------------------------------------------------
  // In this function, you are supposed to get into timeline mode.
  // You may need to call a service method to communicate with
  // the server. Use getPostMessage/displayPostMessage functions
  // in client.cc file for both getting and displaying messages
  // in timeline mode.
  // ------------------------------------------------------------

  // ------------------------------------------------------------
  // IMPORTANT NOTICE:
  //
  // Once a user enter to timeline mode , there is no way
  // to command mode. You don't have to worry about this situation,
  // and you can terminate the client program by pressing
  // CTRL-C (SIGINT)
  // ------------------------------------------------------------

  ClientContext context;
  context.AddMetadata("token", session_token);

  std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
      stub_->Timeline(&context));
  // check if stream is valid
  if (!stream) {
    std::cout << "Failed to create stream\n";
    return;
  }
  // thread for user input
  std::thread writer([stream, username]() {
    // while stream is open
    while (stream->Write(MakeMessage(username, getPostMessage()))) {
    };
  });

  // thread for server messages
  Message m;
  while (stream->Read(&m)) {
    time_t t = m.timestamp().seconds();
    displayPostMessage(m.username(), m.msg(), t);
  }
  // should never get here (no way to exit timeline mode)
  writer.join();
}

//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {
  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "3010";

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:p:")) != -1) {
    switch (opt) {
      case 'h':
        hostname = optarg;
        break;
      case 'u':
        username = optarg;
        break;
      case 'p':
        port = optarg;
        break;
      default:
        std::cout << "Invalid Command Line Argument\n";
    }
  }

  std::cout << "Logging Initialized. Client starting...";

  Client myc(hostname, username, port);

  myc.run();

  return 0;
}
