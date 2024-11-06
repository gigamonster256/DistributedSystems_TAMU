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
#include <ostream>
#include <string>

#include "sns.grpc.pb.h"

// message serialization
#define SNS_MESSAGE_TIME_FORMAT "%F %T"
#define USERNAME_PREFIX "http://twitter.com/"

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

typedef ServerReaderWriter<Message, Message> TimelineStream;

std::ostream& operator<<(std::ostream&, const Message&);
std::istream& operator>>(std::istream&, Message&);

struct User {
  std::string username;
  std::mutex mtx;
  std::vector<std::string> followers;
  std::vector<std::string> following;
  std::unique_ptr<TimelineStream> stream;
  bool logged_in;
  bool operator==(const User& c1) const { return (username == c1.username); }
  User(std::string name) : username(name), logged_in(false) {}
};

class SNSServiceImpl final : public SNSService::Service {
  // config data
  std::string database_folder_path;

  // runtime data
  std::unordered_map<std::string, User*> user_db;
  std::mutex db_mtx;

  // helper functions
  std::string user_database_file();
  void load_user_db();
  std::string user_following_file(const std::string& username);
  std::string user_timeline_file(const std::string& username);

  // service methods
  Status Login(ServerContext*, const LoginRequest* request,
               Empty* response) override;
  Status List(ServerContext*, const ListRequest* request,
              ListReply* list_reply) override;
  Status Follow(ServerContext*, const FollowRequest* request, Empty*) override;
  Status UnFollow(ServerContext*, const FollowRequest* request,
                  Empty*) override;
  Status Timeline(ServerContext* context, TimelineStream* stream) override;
  Status Ping(ServerContext*, const Empty* request, Empty* response) override;

 public:
  SNSServiceImpl(const std::string& server_folder)
      : database_folder_path(server_folder) {
    load_user_db();
  }
};

Status SNSServiceImpl::List(ServerContext*, const ListRequest* request,
                            ListReply* list_reply) {
  User* user = user_db[request->username()];
  if (user == nullptr) {
    log(ERROR, "List: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
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

Status SNSServiceImpl::Follow(ServerContext*, const FollowRequest* request,
                              Empty*) {
  auto following_user = user_db[request->username()];
  if (following_user == nullptr) {
    log(ERROR, "Follow: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
  }
  log(INFO, "Follow request from " + following_user->username);

  const auto& being_followed_username = request->follower();

  log(INFO, "User " + following_user->username + " following " +
                being_followed_username);

  // make sure not following self
  if (following_user->username == being_followed_username) {
    log(ERROR, "Follow: Cannot follow self");
    return Status(grpc::StatusCode::INVALID_ARGUMENT, "Cannot follow self");
  }

  // get the client that is being followed
  auto being_followed_client = user_db[being_followed_username];
  if (being_followed_client == nullptr) {
    log(ERROR, "Follow: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "Following user not found");
  }

  // make sure not already following
  for (auto& already_followed_user : following_user->following) {
    if (already_followed_user == being_followed_username) {
      log(ERROR, "Follow: Already following");
      return Status(grpc::StatusCode::ALREADY_EXISTS, "Already following");
    }
  }

  // add to following lists
  {
    std::lock_guard<std::mutex> lock(following_user->mtx);
    following_user->following.push_back(being_followed_username);
  }
  {
    std::lock_guard<std::mutex> lock(being_followed_client->mtx);
    being_followed_client->followers.push_back(following_user->username);
  }

  // save following to file
  std::lock_guard<std::mutex> lock(following_user->mtx);
  auto filename = user_following_file(following_user->username);
  std::ofstream file(filename);
  if (file.is_open()) {
    for (auto& username : following_user->following) {
      file << username << std::endl;
    }
    file.close();
  }

  return Status::OK;
}

Status SNSServiceImpl::UnFollow(ServerContext*, const FollowRequest* request,
                                Empty*) {
  auto unfollowing_user = user_db[request->username()];
  if (unfollowing_user == nullptr) {
    log(ERROR, "UnFollow: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "User not found");
  }
  log(INFO, "Unfollow request from " + unfollowing_user->username);

  // get the user that this user wants to unfollow
  const auto& being_unfollowed_username = request->follower();

  // make sure not unfollowing self
  if (unfollowing_user->username == being_unfollowed_username) {
    log(ERROR, "UnFollow: Cannot unfollow self");
    return Status(grpc::StatusCode::INVALID_ARGUMENT, "Cannot unfollow self");
  }

  // get the client that is being unfollowed
  auto being_unfollowed_user = user_db[being_unfollowed_username];
  if (being_unfollowed_user == nullptr) {
    log(ERROR, "UnFollow: User not found");
    return Status(grpc::StatusCode::NOT_FOUND, "Following user not found");
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
  {
    std::lock_guard<std::mutex> lock(unfollowing_user->mtx);
    for (auto it = unfollowing_user->following.begin();
         it != unfollowing_user->following.end(); it++) {
      if (*it == being_unfollowed_username) {
        unfollowing_user->following.erase(it);
        break;
      }
    }
  }
  {
    std::lock_guard<std::mutex> lock(being_unfollowed_user->mtx);
    for (auto it = being_unfollowed_user->followers.begin();
         it != being_unfollowed_user->followers.end(); it++) {
      if (*it == unfollowing_user->username) {
        being_unfollowed_user->followers.erase(it);
        break;
      }
    }
  }

  // save following to file
  {
    std::lock_guard<std::mutex> lock(unfollowing_user->mtx);
    auto filename = user_following_file(unfollowing_user->username);
    std::ofstream file(filename);
    if (file.is_open()) {
      for (auto& username : unfollowing_user->following) {
        file << username << std::endl;
      }
      file.close();
    }
  }

  // clear timeline of posts
  {
    std::lock_guard<std::mutex> lock(unfollowing_user->mtx);
    std::vector<Message> messages;
    auto filename = user_timeline_file(unfollowing_user->username);
    std::ifstream old_file(filename);
    if (old_file.is_open()) {
      Message message;
      while (true) {
        old_file >> message;
        std::string line;
        std::getline(old_file, line);
        if (old_file.eof()) {
          break;
        }
        if (message.username() != being_unfollowed_username) {
          messages.push_back(message);
        }
      }
      old_file.close();
    }

    // garbage collect
    if (messages.size() > 20) {
      messages.erase(messages.begin(), messages.end() - 20);
    }

    // save to file
    auto new_file = std::ofstream(filename);
    if (new_file.is_open()) {
      for (auto& message : messages) {
        new_file << message << std::endl;
      }
      new_file.close();
    }
  }

  return Status::OK;
}

Status SNSServiceImpl::Login(ServerContext*, const LoginRequest* request,
                             Empty*) {
  const std::string& username = request->username();
  log(INFO, "Login request from " + username);

  // get/create user
  User* user = nullptr;
  {
    std::lock_guard<std::mutex> lock(db_mtx);
    user = user_db[username];
    if (user == nullptr) {
      log(INFO, "New user " + username);
      user = user_db[username] = new User(username);
      // populate
      auto filename = user_database_file();
      std::ofstream file(filename, std::ios::app);
      if (file.is_open()) {
        file << username << std::endl;
        file.close();
      }
    }
  }
  // check if user is already logged in
  if (user->logged_in) {
    log(ERROR, "Login: User already logged in");
    return Status(grpc::StatusCode::ALREADY_EXISTS, "User already logged in");
  }

  // set logged in
  user->logged_in = true;

  log(INFO, "User " + username + " connected");

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
  User* user = user_db[username];

  if (user == nullptr) {
    log(ERROR, "Timeline: Invalid user");
    return Status(grpc::StatusCode::FAILED_PRECONDITION, "Invalid user");
  }

  log(INFO, "Timeline request from " + user->username);

  user->stream = std::unique_ptr<TimelineStream>(stream);

  // send the user their timeline
  {
    std::lock_guard<std::mutex> lock(user->mtx);
    auto filename = user_timeline_file(user->username);
    std::ifstream file(filename);
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
      auto filename = user_timeline_file(user->username);
      std::ofstream file(filename, std::ios::app);
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
        auto filename = user_timeline_file(follower);
        std::ofstream file(filename, std::ios::app);
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

Status SNSServiceImpl::Ping(ServerContext*, const Empty*, Empty*) {
  return Status::OK;
}

void SNSServiceImpl::load_user_db() {
  auto userfile = user_database_file();
  std::ifstream file(userfile);
  if (file.is_open()) {
    std::string username;
    while (std::getline(file, username)) {
      user_db[username] = new User(username);
    }
    file.close();
  }
  // populate following
  for (auto& [username, user] : user_db) {
    auto followingfile = user_following_file(username);
    std::ifstream file(followingfile);
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

std::string SNSServiceImpl::user_database_file() {
  return database_folder_path + "users.txt";
}

std::string SNSServiceImpl::user_following_file(const std::string& username) {
  return database_folder_path + username + ".following";
}

std::string SNSServiceImpl::user_timeline_file(const std::string& username) {
  return database_folder_path + username + ".timeline";
}

void RunServer(const std::string& server_folder, uint32_t port) {
  std::string server_address = "0.0.0.0:" + std::to_string(port);
  SNSServiceImpl service(server_folder);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on " + server_address);

  server->Wait();
}

int main(int argc, char** argv) {
  uint32_t port = 9090;

  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1) {
    switch (opt) {
      case 'p':
        port = std::stoi(optarg);
        break;
      default:
        std::cerr << "Invalid Command Line Argument" << std::endl;
    }
  }

  // create the server folder
  std::string server_folder = "server/";
  if (mkdir(server_folder.c_str(), 0777) == -1) {
    if (errno != EEXIST) {
      log(ERROR, "Failed to create server folder");
      perror("mkdir");
      return 1;
    }
  }

  // initialize logging
  std::string log_file_name = server_folder + "server.log";
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");

  RunServer(server_folder, port);

  return 0;
}

std::ostream& operator<<(std::ostream& file, const Message& message) {
  const time_t time = message.timestamp().seconds();
  const struct tm* timeinfo = std::localtime(&time);
  file << "T " << std::put_time(timeinfo, SNS_MESSAGE_TIME_FORMAT) << std::endl
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
  strptime(time_str.c_str(), SNS_MESSAGE_TIME_FORMAT, &timeinfo);
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
