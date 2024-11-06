#include "client.h"

#include <algorithm>
#include <optional>
#include <string>

SNSCommandType getCommandType(std::string cmd) {
  std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
  if (cmd == "LIST") {
    return LIST;
  }
  if (cmd == "FOLLOW") {
    return FOLLOW;
  }
  if (cmd == "UNFOLLOW") {
    return UNFOLLOW;
  }
  if (cmd == "TIMELINE") {
    return TIMELINE;
  }
  return INVALID;
}

void IClient::run() {
  auto status = connect();
  if (status != SUCCESS) {
    std::cout << "connection failed" << std::endl;
    exit(1);
  }
  displayTitle();
  while (1) {
    auto cmd = getCommand();
    auto reply = processCommand(cmd);
    displayCommandReply(cmd.first, reply);
    if (cmd.first == TIMELINE && reply.first == SUCCESS) {
      // should never get here (no way to exit timeline mode)
      assert(0);
    }
  }
}

void IClient::displayTitle() const {
  std::cout << std::endl;
  std::cout << "========= TINY SNS CLIENT =========" << std::endl;
  std::cout << " Command Lists and Format:" << std::endl;
  std::cout << " FOLLOW <username>" << std::endl;
  std::cout << " UNFOLLOW <username>" << std::endl;
  std::cout << " LIST" << std::endl;
  std::cout << " TIMELINE" << std::endl;
  std::cout << "=====================================" << std::endl;
}

const SNSCommand IClient::getCommand() const {
  while (1) {
    std::string input;
    std::cout << "Cmd> ";
    std::getline(std::cin, input);
    // trim whitespace after
    input.erase(input.find_last_not_of(" \n\r\t") + 1);
    auto space_index = input.find_first_of(" ");

    // command with argument
    if (space_index != std::string::npos) {
      auto type = getCommandType(input.substr(0, space_index));
      if (type == INVALID) {
        std::cout << "Invalid Command" << std::endl;
        continue;
      }
      std::string argument =
          input.substr(space_index + 1, (input.length() - space_index));
      return std::make_pair(type, argument);
    }
    // command without argument
    else {
      auto type = getCommandType(input);
      if (type == INVALID) {
        std::cout << "Invalid Command" << std::endl;
        continue;
      }
      if (type != LIST && type != TIMELINE) {
        std::cout << "Invalid Input -- No Arguments Given" << std::endl;
        continue;
      }
      return std::make_pair(type, std::nullopt);
    }
  }
}

void IClient::displayCommandReply(SNSCommandType comm,
                                  const SNSReply& reply) const {
  switch (reply.first) {
    case SUCCESS:
      std::cout << "Command completed successfully" << std::endl;
      if (comm == LIST) {
        assert(reply.second.has_value());
        const auto& users = reply.second.value()->first;
        const auto& followers = reply.second.value()->second;
        std::cout << "All users: ";
        for (std::string room : users) {
          std::cout << room << ", ";
        }
        std::cout << std::endl;
        std::cout << "Followers: ";
        for (std::string room : followers) {
          std::cout << room << ", ";
        }
        std::cout << std::endl;
      }
      break;
    case FAILURE_ALREADY_EXISTS:
      std::cout << "Input username already exists, command failed" << std::endl;
      break;
    case FAILURE_NOT_EXISTS:
      std::cout << "Input username does not exist, command failed" << std::endl;
      break;
    case FAILURE_INVALID_USERNAME:
      std::cout << "Command failed with invalid username" << std::endl;
      break;
    case FAILURE_NOT_A_FOLLOWER:
      std::cout << "Command failed with not a follower" << std::endl;
      break;
    case FAILURE_INVALID:
      std::cout << "Command failed with invalid command" << std::endl;
      break;
    case FAILURE_UNKNOWN:
      std::cout << "Command failed with unknown reason" << std::endl;
      break;
    default:
      std::cout << "Invalid status" << std::endl;
      break;
  }
}

/*
 * get/displayPostMessage functions will be called in chatmode
 */
std::string getPostMessage() {
  char buf[MAX_DATA];
  while (1) {
    fgets(buf, MAX_DATA, stdin);
    if (buf[0] != '\n') break;
  }

  std::string message(buf);
  return message;
}

void displayPostMessage(const std::string& sender, const std::string& message,
                        std::time_t& time) {
  std::string t_str(std::ctime(&time));
  t_str[t_str.size() - 1] = '\0';
  std::cout << sender << " (" << t_str << ") >> " << message << std::endl;
}
