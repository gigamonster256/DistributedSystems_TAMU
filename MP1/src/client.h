#include <grpc++/grpc++.h>

#include <ctime>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#define MAX_DATA 256

enum SNSStatus {
  SUCCESS,
  FAILURE_ALREADY_EXISTS,
  FAILURE_NOT_EXISTS,
  FAILURE_INVALID_USERNAME,
  FAILURE_NOT_A_FOLLOWER,
  FAILURE_INVALID,
  FAILURE_UNKNOWN,
};

enum SNSCommandType {
  LIST,
  FOLLOW,
  UNFOLLOW,
  TIMELINE,
  INVALID,
};

typedef std::pair<SNSCommandType, std::optional<std::string>> SNSCommand;
typedef std::pair<std::vector<std::string>, std::vector<std::string>>
    SNSListReply;
typedef std::pair<SNSStatus, std::optional<std::unique_ptr<SNSListReply>>>
    SNSReply;

SNSCommandType getCommandType(std::string cmd);

std::string getPostMessage();
void displayPostMessage(const std::string& sender, const std::string& message,
                        std::time_t& time);

class IClient {
 public:
  void run();

 protected:
  virtual SNSStatus connect() = 0;
  virtual SNSReply processCommand(SNSCommand) = 0;
  virtual void processTimeline() = 0;

 private:
  void displayTitle() const;
  const SNSCommand getCommand() const;
  void displayCommandReply(SNSCommandType cmd, const SNSReply& reply) const;
};
