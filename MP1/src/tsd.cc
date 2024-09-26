/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <stdlib.h>
#include <unistd.h>

#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"

// message serialization
#define TIME_FORMAT ("%F %T")
#define USERNAME_PREFIX "http://twitter.com/"

// database files
#define USERFILE ("users.txt")
#define FOLLOWINGFILEEXTENSION (".following")
#define TIMELINEFILEEXTENSION (".timeline")

using csce662::ListReply;
using csce662::Message;
using csce662::Reply;
using csce662::Request;
using csce662::SNSService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

std::ostream& operator<<(std::ostream&, const Message&);
std::istream& operator>>(std::istream&, Message&);

struct User {
  std::string username;
  std::mutex mtx;
  std::vector<std::string> followers;
  std::vector<std::string> following;
  ServerReaderWriter<Message, Message>* stream = nullptr;
  bool operator==(const User& c1) const { return (username == c1.username); }
  User(std::string name) : username(name) {}
};

typedef std::string session_token;

std::unordered_map<std::string, User*> user_db;
std::unordered_map<session_token, User*> user_sessions;
std::mutex db_mtx;
std::mutex session_mtx;

void load_user_db() {
  std::ifstream file(USERFILE);
  if (file.is_open()) {
    std::string username;
    while (std::getline(file, username)) {
      user_db[username] = new User(username);
    }
    file.close();
  }
  // populate following
  for (auto& [username, user] : user_db) {
    std::ifstream file(username + FOLLOWINGFILEEXTENSION);
    if (file.is_open()) {
      std::string followed_username;
      while (std::getline(file, followed_username)) {
        if (followed_username != "") {
          User* followed_user = user_db[followed_username];
          if (followed_user != nullptr) {
            user->following.push_back(followed_username);
            followed_user->followers.push_back(username);
          } else {
            log(ERROR, "Database: user " << username << " following "
                                         << followed_username
                                         << " who is not in database");
          }
        }
      }
      file.close();
    }
  }
}

session_token generate_session(User* user) {
  session_token token;
  int iterations = 0;
  std::lock_guard<std::mutex> lock(session_mtx);
  while (iterations < 100) {
    token = std::to_string(rand());
    if (user_sessions.find(token) == user_sessions.end()) {
      user_sessions[token] = user;
      return token;
    }
  }
  return "";
}

class SNSServiceImpl final : public SNSService::Service {
  Status List(ServerContext*, const Request* request,
              ListReply* list_reply) override {
    const std::string& token = request->username();
    User* user = user_sessions[token];
    if (user == nullptr) {
      log(ERROR, "List: Bad token");
      return Status(grpc::StatusCode::UNAUTHENTICATED, "Invalid user");
    }
    log(INFO, "List request from " + user->username);
    for (auto& [username, user] : user_db) {
      if (user == nullptr) continue;
      list_reply->add_all_users(username);
    }
    for (auto& username : user->followers) {
      list_reply->add_followers(username);
    }
    return Status::OK;
  }

  Status Follow(ServerContext*, const Request* request, Reply*) override {
    const std::string& token = request->username();
    User* following_user = user_sessions[token];
    if (following_user == nullptr) {
      log(ERROR, "Follow: Bad token");
      return Status(grpc::StatusCode::UNAUTHENTICATED, "Invalid user");
    }
    log(INFO, "Follow request from " + following_user->username);

    // get the user that this user wants to follow
    if (request->arguments_size() == 0) {
      log(ERROR, "Follow: Too few arguments");
      return Status(grpc::StatusCode::INVALID_ARGUMENT, "Too few arguments");
    }
    if (request->arguments_size() > 1) {
      log(ERROR, "Follow: Too many arguments");
      return Status(grpc::StatusCode::INVALID_ARGUMENT, "Too many arguments");
    }
    const std::string& being_followed_username = request->arguments(0);

    // make sure not following self
    if (following_user->username == being_followed_username) {
      log(ERROR, "Follow: Cannot follow self");
      return Status(grpc::StatusCode::INVALID_ARGUMENT, "Cannot follow self");
    }

    // get the client that is being followed
    User* being_followed_client = user_db[being_followed_username];
    if (being_followed_client == nullptr) {
      log(ERROR, "Follow: User not found");
      return Status(grpc::StatusCode::NOT_FOUND, "User not found");
    }

    // make sure not already following
    for (auto& already_followed_user : following_user->following) {
      if (already_followed_user == being_followed_username) {
        log(ERROR, "Follow: Already following");
        return Status(grpc::StatusCode::ALREADY_EXISTS, "Already following");
      }
    }

    // add to following lists
    following_user->mtx.lock();
    following_user->following.push_back(being_followed_username);
    following_user->mtx.unlock();

    being_followed_client->mtx.lock();
    being_followed_client->followers.push_back(following_user->username);
    being_followed_client->mtx.unlock();

    // save following to file
    std::lock_guard<std::mutex> lock(following_user->mtx);
    std::ofstream file(following_user->username + FOLLOWINGFILEEXTENSION);
    if (file.is_open()) {
      for (auto& username : following_user->following) {
        file << username << std::endl;
      }
      file.close();
    }

    return Status::OK;
  }

  Status UnFollow(ServerContext*, const Request* request, Reply*) override {
    const std::string& token = request->username();
    User* unfollowing_user = user_sessions[token];
    if (unfollowing_user == nullptr) {
      log(ERROR, "UnFollow: Bad token");
      return Status(grpc::StatusCode::UNAUTHENTICATED, "Invalid user");
    }
    log(INFO, "Unfollow request from " + unfollowing_user->username);

    // get the user that this user wants to unfollow
    if (request->arguments_size() == 0) {
      log(ERROR, "UnFollow: Too few arguments");
      return Status(grpc::StatusCode::INVALID_ARGUMENT, "Too few arguments");
    }
    if (request->arguments_size() > 1) {
      log(ERROR, "UnFollow: Too many arguments");
      return Status(grpc::StatusCode::INVALID_ARGUMENT, "Too many arguments");
    }
    std::string being_unfollowed_username = request->arguments(0);

    // make sure not unfollowing self
    if (unfollowing_user->username == being_unfollowed_username) {
      log(ERROR, "UnFollow: Cannot unfollow self");
      return Status(grpc::StatusCode::INVALID_ARGUMENT, "Cannot unfollow self");
    }

    // get the client that is being unfollowed
    User* being_unfollowed_user = user_db[being_unfollowed_username];
    if (being_unfollowed_user == nullptr) {
      log(ERROR, "UnFollow: User not found");
      return Status(grpc::StatusCode::NOT_FOUND, "User not found");
    }

    // make sure already following
    bool found = false;
    for (auto& username : unfollowing_user->following) {
      if (username == being_unfollowed_username) {
        found = true;
        break;
      }
    }
    if (!found) {
      log(ERROR, "UnFollow: Not following");
      return Status(grpc::StatusCode::NOT_FOUND, "Not following");
    }

    // remove from following lists
    unfollowing_user->mtx.lock();
    for (auto it = unfollowing_user->following.begin();
         it != unfollowing_user->following.end(); it++) {
      if (*it == being_unfollowed_username) {
        unfollowing_user->following.erase(it);
        break;
      }
    }
    unfollowing_user->mtx.unlock();

    being_unfollowed_user->mtx.lock();
    for (auto it = being_unfollowed_user->followers.begin();
         it != being_unfollowed_user->followers.end(); it++) {
      if (*it == unfollowing_user->username) {
        being_unfollowed_user->followers.erase(it);
        break;
      }
    }
    being_unfollowed_user->mtx.unlock();

    // save follwing to file
    std::lock_guard<std::mutex> lock(unfollowing_user->mtx);
    std::ofstream file(unfollowing_user->username + FOLLOWINGFILEEXTENSION);
    if (file.is_open()) {
      for (auto& username : unfollowing_user->following) {
        file << username << std::endl;
      }
      file.close();
    }

    // clear timeline of posts
    std::vector<Message> messages;
    std::ifstream file2(unfollowing_user->username + TIMELINEFILEEXTENSION);
    if (file2.is_open()) {
      Message message;
      while (true) {
        file2 >> message;
        std::string line;
        std::getline(file2, line);
        if (file2.eof()) {
          break;
        }
        if (message.username() != being_unfollowed_username) {
          messages.push_back(message);
        }
      }
      file2.close();
    }

    // garbage collect
    if (messages.size() > 20) {
      messages.erase(messages.begin(), messages.end() - 20);
    }

    // save to file
    std::ofstream file3(unfollowing_user->username + TIMELINEFILEEXTENSION);
    if (file3.is_open()) {
      for (auto& message : messages) {
        file3 << message << std::endl;
      }
      file3.close();
    }

    return Status::OK;
  }

  Status Login(ServerContext*, const Request* request, Reply* reply) override {
    const std::string& username = request->username();
    log(INFO, "Login request from " + username);

    // get/create user
    db_mtx.lock();
    User*& user = user_db[username];
    if (user == nullptr) {
      log(INFO, "New user " + username);
      user = new User(username);
      // populate
      std::ofstream file(USERFILE, std::ios::app);
      if (file.is_open()) {
        file << username << std::endl;
        file.close();
      }
    }
    db_mtx.unlock();

    for (auto& [_, logged_in_user] : user_sessions) {
      if (*logged_in_user == *user) {
        log(ERROR, "Login: User already logged in");
        return Status(grpc::StatusCode::ALREADY_EXISTS,
                      "User already logged in");
      }
    }

    log(INFO, "User " + username + " connected");

    // generate a session token
    session_token token = generate_session(user);
    if (token.empty()) {
      log(ERROR, "Login: Failed to generate session token");
      return Status(grpc::StatusCode::INTERNAL,
                    "Failed to generate session token");
    }
    reply->set_msg(token);

    return Status::OK;
  }

  Status Timeline(ServerContext* context,
                  ServerReaderWriter<Message, Message>* stream) override {
    // get session token from client metadata
    const auto& metadata = context->client_metadata();
    std::string token;
    for (auto it = metadata.begin(); it != metadata.end(); it++) {
      if (it->first == "token") {
        token = std::string(it->second.data(), it->second.size());
        break;
      }
    }
    if (token.empty()) {
      log(ERROR, "Timeline: Username not found");
      stream->WriteLast(Message(), grpc::WriteOptions());
      return Status(grpc::StatusCode::FAILED_PRECONDITION, "Username missing");
    }

    // save the stream so that who we are following can send us messages
    User* user = user_sessions[token];

    if (user == nullptr) {
      log(ERROR, "Timeline: Invalid user");
      stream->WriteLast(Message(), grpc::WriteOptions());
      return Status(grpc::StatusCode::FAILED_PRECONDITION, "Invalid user");
    }

    log(INFO, "Timeline request from " + user->username);

    user->stream = stream;

    // send the user their timeline
    {
      std::lock_guard<std::mutex> lock(user->mtx);
      std::ifstream file(user->username + TIMELINEFILEEXTENSION);
      if (file.is_open()) {
        std::vector<Message> messages;
        Message message;
        while (true) {
          file >> message;
          std::string line;
          std::getline(file, line);
          if (file.eof()) {
            break;
          }
          messages.push_back(message);
        }
        // truncate to most resent 20
        if (messages.size() > 20) {
          messages.erase(messages.begin(), messages.end() - 20);
        }
        // send in reverse order
        for (auto it = messages.rbegin(); it != messages.rend(); it++) {
          stream->Write(*it);
        }
        file.close();
      }
    }

    // forward messages to followers
    Message message;
    while (stream->Read(&message)) {
      // make sure the user is sending messages as themselves
      const std::string& username = message.username();
      if (username != user->username) {
        log(ERROR, "Timeline: User " + user->username +
                       " trying to send message as " + username);
        stream->WriteLast(Message(), grpc::WriteOptions());
        return Status(grpc::StatusCode::PERMISSION_DENIED,
                      "Cannot send message as another user");
      }
      log(INFO, "Message from " + username + " to followers");
      // save to self file
      {
        std::lock_guard<std::mutex> lock(user->mtx);
        std::ofstream file(user->username + TIMELINEFILEEXTENSION,
                           std::ios::app);
        if (file.is_open()) {
          file << message << std::endl;
          file.close();
        }
      }
      for (auto& follower : user->followers) {
        if (user_db[follower] == nullptr) {
          log(ERROR, "Timeline: Follower " + follower + " not found");
          continue;
        }
        // save to followers files and self file
        {
          std::lock_guard<std::mutex> lock(user_db[follower]->mtx);
          std::ofstream file(follower + TIMELINEFILEEXTENSION, std::ios::app);
          if (file.is_open()) {
            file << message << std::endl;
            file.close();
          }
        }
        // send to followers if they are online
        if (user_db[follower]->stream != nullptr) {
          user_db[follower]->stream->Write(message);
        }
      }
    }

    return Status::OK;
  }
};

void RunServer(const std::string& port_no) {
  std::string server_address = "0.0.0.0:" + port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on " + server_address);

  // load the user database
  load_user_db();
  server->Wait();
}

int main(int argc, char** argv) {
  std::string port = "3010";

  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1) {
    switch (opt) {
      case 'p':
        port = optarg;
        break;
      default:
        std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port);

  return 0;
}

std::ostream& operator<<(std::ostream& file, const Message& message) {
  const time_t time = message.timestamp().seconds();
  const struct tm* timeinfo = std::localtime(&time);
  file << "T " << std::put_time(timeinfo, TIME_FORMAT) << std::endl
       << "U " << USERNAME_PREFIX << message.username() << std::endl
       << "W " << message.msg();
  return file;
}

std::istream& operator>>(std::istream& file, Message& message) {
  std::string line;
  std::getline(file, line);
  if (line.empty() || line[0] != 'T') {
    return file;
  }
  std::string time_str = line.substr(2);
  struct tm timeinfo;
  strptime(time_str.c_str(), TIME_FORMAT, &timeinfo);
  time_t time = mktime(&timeinfo);
  Timestamp* timestamp = new Timestamp();
  timestamp->set_seconds(time);
  message.set_allocated_timestamp(timestamp);
  std::getline(file, line);
  if (line.empty() || line[0] != 'U') {
    return file;
  }
  std::string username = line.substr(2 + strlen(USERNAME_PREFIX));
  message.set_username(username);
  std::getline(file, line);
  if (line.empty() || line[0] != 'W') {
    return file;
  }
  message.set_msg(line.substr(2) + "\n");
  return file;
}
