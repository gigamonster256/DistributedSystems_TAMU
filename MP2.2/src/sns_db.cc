#include "sns_db.h"

namespace csce662 {

std::string SNSUser::get_username() const { return username_; }

void SNSUser::add_follower(const std::string& follower) {
  db_->add_follower(username_, follower);
}

void SNSUser::remove_follower(const std::string& follower) {
  db_->remove_follower(username_, follower);
}

std::vector<std::string> SNSUser::get_following() const {
  return db_->get_following(username_);
}

std::vector<std::string> SNSUser::get_followers() const {
  return db_->get_followers(username_);
}

std::vector<Message> SNSUser::get_timeline() const {
  return db_->get_timeline(username_);
}

void SNSUser::post(const Message& message) {
  db_->post_to_self_and_followers(username_, message);
}

void SNSUser::add_message(const Message& message) {
  db_->post_to_timeline(username_, message);
}

std::string SNSDatabase::user_database_file() const {
  return folder_path_ + "users.txt";
}

std::string SNSDatabase::user_following_file(
    const std::string& username) const {
  return folder_path_ + username + ".following";
}

std::string SNSDatabase::user_followers_file(
    const std::string& username) const {
  return folder_path_ + username + ".followers";
}

std::string SNSDatabase::user_timeline_file(const std::string& username) const {
  return folder_path_ + username + ".timeline";
}

std::vector<std::string> SNSDatabase::get_all_usernames() const {
  std::vector<std::string> all_users;
  auto userfile = user_database_file();
  ensure_file_exists(userfile);
  ExclusiveInputFileStream file(userfile);
  std::string username;
  while (std::getline(file, username)) {
    all_users.push_back(username);
  }
  return all_users;
}

void SNSDatabase::create_user_if_not_exists(const std::string& username) {
  auto all_users = get_all_usernames();
  if (std::find(all_users.begin(), all_users.end(), username) ==
      all_users.end()) {
    create_user(username);
  }
}

void SNSDatabase::create_user(const std::string& username) {
  auto userfile = user_database_file();
  ExclusiveOutputFileStream file(userfile, std::ios::app);
  file << username << std::endl;
}

std::optional<SNSUser> SNSDatabase::get_user(const std::string& username) {
  auto all_users = get_all_usernames();
  if (std::find(all_users.begin(), all_users.end(), username) ==
      all_users.end()) {
    return std::nullopt;
  }
  return SNSUser(username, this);
}

void SNSDatabase::add_follower(const std::string& username,
                               const std::string& follower) {
  auto following = get_following(username);
  if (std::find(following.begin(), following.end(), follower) !=
      following.end()) {
    return;
  }
  auto followingfile = user_following_file(username);
  ExclusiveOutputFileStream followingf(followingfile, std::ios::app);
  followingf << follower << std::endl;

  auto followersfile = user_followers_file(follower);
  ExclusiveOutputFileStream followersf(followersfile, std::ios::app);
  followersf << username << std::endl;
}

std::vector<std::string> SNSDatabase::get_following(
    const std::string& username) const {
  std::vector<std::string> following;
  auto followingfile = user_following_file(username);
  ensure_file_exists(followingfile);
  ExclusiveInputFileStream file(followingfile);
  std::string followed_username;
  while (std::getline(file, followed_username)) {
    following.push_back(followed_username);
  }
  return following;
}

std::vector<std::string> SNSDatabase::get_followers(
    const std::string& username) const {
  std::vector<std::string> followers;
  auto followersfile = user_followers_file(username);
  ensure_file_exists(followersfile);
  ExclusiveInputFileStream file(followersfile);
  std::string following_username;
  while (std::getline(file, following_username)) {
    followers.push_back(following_username);
  }
  return followers;
}

void SNSDatabase::remove_follower(const std::string& username,
                                  const std::string& follower) {
  auto followers = get_followers(username);
  followers.erase(std::remove(followers.begin(), followers.end(), follower),
                  followers.end());
  auto followersfile = user_followers_file(username);
  ExclusiveOutputFileStream file(followersfile);
  for (const auto& follower : followers) {
    file << follower << std::endl;
  }

  auto following = get_following(follower);
  following.erase(std::remove(following.begin(), following.end(), username),
                  following.end());
  auto followingfile = user_following_file(follower);
  ExclusiveOutputFileStream file2(followingfile);
  for (const auto& followed : following) {
    file2 << followed << std::endl;
  }
}

std::vector<Message> SNSDatabase::get_timeline(
    const std::string& username) const {
  std::vector<Message> timeline;
  auto timelinefile = user_timeline_file(username);
  ensure_file_exists(timelinefile);
  ExclusiveInputFileStream file(timelinefile);
  Message message;
  while (file >> message) {
    timeline.push_back(message);
  }
  return timeline;
}

void SNSDatabase::post_to_timeline(const std::string& username,
                                   const Message& message) {
  auto timelinefile = user_timeline_file(username);
  ExclusiveOutputFileStream file(timelinefile, std::ios::app);
  file << message;
}

void SNSDatabase::post_to_self_and_followers(const std::string& username,
                                             const Message& message) {
  post_to_timeline(username, message);
  auto followers = get_followers(username);
  for (const auto& follower : followers) {
    post_to_timeline(follower, message);
  }
}

void SNSDatabase::ensure_file_exists(const std::string& filename) const {
  std::ifstream file(filename);
  if (!file.good()) {
    std::ofstream newfile(filename);
  }
}

}  // namespace csce662

std::ostream& operator<<(std::ostream& file, const csce662::Message& message) {
  const time_t time = message.timestamp().seconds();
  struct tm* timeinfo = std::localtime(&time);
  timeinfo->tm_isdst = 0;
  file << "T " << std::put_time(timeinfo, SNS_MESSAGE_TIME_FORMAT) << std::endl
       << "U " << USERNAME_PREFIX << message.username() << std::endl
       << "W " << message.msg();
  return file;
}

std::istream& operator>>(std::istream& file, csce662::Message& message) {
  std::string line;
  std::getline(file, line);
  if (line.empty() || line[0] != 'T') {
    return file;
  }
  std::string time_str = line.substr(2);
  struct tm timeinfo;
  strptime(time_str.c_str(), SNS_MESSAGE_TIME_FORMAT, &timeinfo);
  timeinfo.tm_isdst = 0;
  time_t time = mktime(&timeinfo);
  auto timestamp = new google::protobuf::Timestamp();
  timestamp->set_seconds(time);
  timestamp->set_nanos(0);
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