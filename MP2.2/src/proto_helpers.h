#pragma once

#include <string>
#include <tuple>

#include "coordinator.pb.h"

std::tuple<const std::string&, uint32_t> get_server_address(
    const csce662::ServerInfo& server_info) {
  return {server_info.hostname(), server_info.port()};
}