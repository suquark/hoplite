#ifndef SOCKET_UTILS_H
#define SOCKET_UTILS_H

#include <string>

int send_all(int conn_fd, const void *buf, const size_t size);

int recv_all(int conn_fd, void *buf, const size_t size);

int tcp_connect(const std::string &ip_address, int port, int *conn_fd);

void tcp_bind_and_listen(int port, struct sockaddr_in *address, int *server_fd);

#endif // SOCKET_UTILS_H
