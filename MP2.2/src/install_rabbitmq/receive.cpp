#include <amqp.h>
#include <amqp_tcp_socket.h>

#include <iostream>

int main() {
  const char* hostname = "localhost";
  int port = 5672;
  const char* queue_name = "queuename";

  amqp_connection_state_t conn = amqp_new_connection();
  amqp_socket_t* socket = amqp_tcp_socket_new(conn);

  if (!socket) {
    std::cerr << "Could not create TCP socket\n";
    return 1;
  }

  if (amqp_socket_open(socket, hostname, port)) {
    std::cerr << "Could not open TCP socket\n";
    return 1;
  }

  amqp_rpc_reply_t reply = amqp_login(conn, "/", 0, 131072, 0,
                                      AMQP_SASL_METHOD_PLAIN, "guest", "guest");
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    std::cerr << "Failed to login\n";
    return 1;
  }

  amqp_channel_open(conn, 1);
  amqp_get_rpc_reply(conn);

  amqp_bytes_t queue = amqp_cstring_bytes(queue_name);
  amqp_queue_declare(conn, 1, queue, 0, 0, 0, 1, amqp_empty_table);
  amqp_get_rpc_reply(conn);

  amqp_basic_consume(conn, 1, queue, amqp_empty_bytes, 0, 1, 0,
                     amqp_empty_table);
  amqp_get_rpc_reply(conn);

  std::cout << " [*] Waiting for messages. To exit press CTRL+C\n";

  while (true) {
    amqp_rpc_reply_t res;
    amqp_envelope_t envelope;

    amqp_maybe_release_buffers(conn);
    res = amqp_consume_message(conn, &envelope, nullptr, 0);

    if (res.reply_type == AMQP_RESPONSE_NORMAL) {
      std::string body(static_cast<char*>(envelope.message.body.bytes),
                       envelope.message.body.len);
      std::cout << " [x] Received '" << body << "'\n";

      amqp_destroy_envelope(&envelope);
    } else {
      std::cerr << "Failed to consume message\n";
      break;
    }
  }

  amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
  amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(conn);

  return 0;
}
