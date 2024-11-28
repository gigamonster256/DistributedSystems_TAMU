#pragma once

#include <fcntl.h>
#include <google/protobuf/timestamp.pb.h>
#include <sys/file.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "sns.pb.h"

// message serialization
#define SNS_MESSAGE_TIME_FORMAT "%F %T"
#define USERNAME_PREFIX "http://twitter.com/"

std::ostream& operator<<(std::ostream&, const csce662::Message&);
std::istream& operator>>(std::istream&, csce662::Message&);

namespace csce662 {

class FileOutputBuffer : public std::streambuf {
 public:
  explicit FileOutputBuffer(FILE* file) : file_(file) {}

 protected:
  int overflow(int ch) override {
    if (ch == EOF) {
      return EOF;
    }
    if (fputc(ch, file_) == EOF) {
      return EOF;
    }
    return ch;
  }

  int sync() override { return fflush(file_) == 0 ? 0 : -1; }

 private:
  FILE* file_;
};

class ExclusiveOutputFileStream : public std::ostream {
 public:
  ExclusiveOutputFileStream(const std::string& filename,
                            std::ios_base::openmode mode = std::ios_base::trunc)
      : std::ostream(nullptr), file_(nullptr), buffer_(nullptr) {
    // Open file in append mode initially until the lock is acquired
    file_ = fopen(filename.c_str(), "a");
    if (file_ == nullptr) {
      throw std::runtime_error("Failed to open file: " + filename);
    }
    if (flock(fileno(file_), LOCK_EX) == -1) {
      fclose(file_);
      throw std::runtime_error("Failed to lock file: " + filename);
    }

    // If the file is opened in trunc mode, truncate the file
    if (mode & std::ios_base::trunc) {
      if (freopen(filename.c_str(), "w", file_) == nullptr) {
        fclose(file_);
        throw std::runtime_error("Failed to truncate file: " + filename);
      }
    }

    // Initialize the custom buffer and set it as the stream buffer
    buffer_ = new FileOutputBuffer(file_);
    this->rdbuf(buffer_);
  }

  ~ExclusiveOutputFileStream() {
    this->flush();  // Flush the stream before releasing the lock
    flock(fileno(file_), LOCK_UN);
    fclose(file_);
    delete buffer_;
  }

 private:
  FILE* file_;
  FileOutputBuffer* buffer_;
};

class FileInputBuffer : public std::streambuf {
 public:
  explicit FileInputBuffer(FILE* file) : file_(file) {}

 protected:
  int underflow() override {
    if (gptr() == egptr()) {  // buffer is empty
      int c = fgetc(file_);
      if (c == EOF) {
        return EOF;
      }
      buffer_ = static_cast<char>(c);
      setg(&buffer_, &buffer_, &buffer_ + 1);  // set buffer pointers
    }
    return traits_type::to_int_type(*gptr());
  }

 private:
  FILE* file_;
  char buffer_;
};

class ExclusiveInputFileStream : public std::istream {
 public:
  ExclusiveInputFileStream(const std::string& filename)
      : std::istream(nullptr), file_(nullptr), buffer_(nullptr) {
    file_ = fopen(filename.c_str(), "r");
    if (file_ == nullptr) {
      throw std::runtime_error("Failed to open file: " + filename);
    }
    if (flock(fileno(file_), LOCK_EX) == -1) {
      fclose(file_);
      throw std::runtime_error("Failed to lock file: " + filename);
    }

    // Initialize the custom buffer and set it as the stream buffer
    buffer_ = new FileInputBuffer(file_);
    this->rdbuf(buffer_);
  }

  ~ExclusiveInputFileStream() {
    flock(fileno(file_), LOCK_UN);
    fclose(file_);
    delete buffer_;
  }

 private:
  FILE* file_;
  FileInputBuffer* buffer_;
};

class SNSDatabase;

class SNSUser {
 private:
  std::string username_;
  SNSDatabase* db_;

 public:
  SNSUser(const std::string& username, SNSDatabase* db)
      : username_(username), db_(db) {}
  // get the username of the user
  std::string get_username() const;
  // add a follower to the user
  void add_follower(const std::string& follower);
  // remove a follower from the user
  void remove_follower(const std::string& follower);
  // who the user is following
  std::vector<std::string> get_following() const;
  // who is following the user
  std::vector<std::string> get_followers() const;
  // get timeline of the user
  std::vector<Message> get_timeline() const;
  // post a message to the user's timeline
  // and the timeline of the user's followers
  void post(const Message& message);
  void add_message(const Message& message);
};

class SNSDatabase {
 private:
  std::string folder_path_;

 public:
  SNSDatabase(const std::string& folder_path) : folder_path_(folder_path) {}
  void create_user_if_not_exists(const std::string& username);
  std::vector<std::string> get_all_usernames() const;
  std::optional<SNSUser> get_user(const std::string& username);
  std::optional<SNSUser> operator[](const std::string& username) {
    return get_user(username);
  }

 private:
  std::string user_database_file() const;
  std::string user_following_file(const std::string& username) const;
  std::string user_followers_file(const std::string& username) const;
  std::string user_timeline_file(const std::string& username) const;
  void create_user(const std::string& username);
  void add_follower(const std::string& username, const std::string& follower);
  void remove_follower(const std::string& username,
                       const std::string& follower);
  std::vector<std::string> get_following(const std::string& username) const;
  std::vector<std::string> get_followers(const std::string& username) const;
  std::vector<Message> get_timeline(const std::string& username) const;
  void post_to_timeline(const std::string& username, const Message& message);
  void post_to_self_and_followers(const std::string& username,
                                  const Message& message);
  void ensure_file_exists(const std::string& filename) const;

  friend class SNSUser;
};

}  // namespace csce662