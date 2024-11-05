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

    amqp_rpc_reply_t reply = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Failed to login\n";
        return 1;
    }

    amqp_channel_open(conn, 1);
    amqp_get_rpc_reply(conn);

    amqp_bytes_t queue = amqp_cstring_bytes(queue_name);
    amqp_queue_declare(conn, 1, queue, 0, 0, 0, 1, amqp_empty_table);
    amqp_get_rpc_reply(conn);

    const char* message = "Hello World!";
    amqp_bytes_t message_bytes = amqp_cstring_bytes(message);

    amqp_basic_publish(conn, 1, amqp_empty_bytes, queue, 0, 0, nullptr, message_bytes);

    std::cout << " [x] Sent 'Hello World!'\n";

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}

