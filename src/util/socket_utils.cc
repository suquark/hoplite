#include "logging.h"
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <ctime>
#include <errno.h>
#include <iostream>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <signal.h>
#include <stdlib.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <zlib.h>

#include "logging.h"

constexpr int BACKLOG = 10;

int send_all(int conn_fd, const void *buf, const size_t size) {
  size_t cursor = 0;
  while (cursor < size) {
    int bytes_sent =
        send(conn_fd, (const uint8_t *)buf + cursor, size - cursor, 0);
    if (bytes_sent < 0) {
      LOG(ERROR) << "Socket send error (" << strerror(errno)
                 << ", code=" << errno << ")";
      if (errno == EAGAIN) {
        continue;
      }
      return bytes_sent;
    }
    cursor += bytes_sent;
  }
  return 0;
}

int recv_all(int conn_fd, void *buf, const size_t size) {
  size_t cursor = 0;
  while (cursor < size) {
    int bytes_recv = recv(conn_fd, (uint8_t *)buf + cursor, size - cursor, 0);
    if (bytes_recv < 0) {
      if (errno == EAGAIN) {
        continue;
      }
      return bytes_recv;
    }
    cursor += bytes_recv;
  }
  return 0;
}

int tcp_connect(const std::string &ip_address, int port, int *conn_fd) {
  struct sockaddr_in push_addr;
  *conn_fd = socket(AF_INET, SOCK_STREAM, 0);
  DCHECK(*conn_fd >= 0) << "socket creation error";
  push_addr.sin_family = AF_INET;
  push_addr.sin_addr.s_addr = inet_addr(ip_address.c_str());
  push_addr.sin_port = htons(port);
  LOG(DEBUG) << "create a connection to " << ip_address;

  int status =
      connect(*conn_fd, (struct sockaddr *)&push_addr, sizeof(push_addr));
  return status;
}

void tcp_bind_and_listen(int port, struct sockaddr_in *address,
                         int *server_fd) {
  *server_fd = socket(AF_INET, SOCK_STREAM, 0);
  DCHECK(*server_fd >= 0) << "socket creation error";

  int enable = 1;
  auto status =
      setsockopt(*server_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));
  DCHECK(!status) << "Cannot set server_fd to reuse port (errno = " << errno
                  << ").";

  address->sin_family = AF_INET;
  address->sin_addr.s_addr = INADDR_ANY;
  address->sin_port = htons(port);

  status = bind(*server_fd, (struct sockaddr *)address, sizeof(*address));
  DCHECK(!status) << "Cannot bind to port " << port << " (errno = " << errno
                  << ").";

  status = listen(*server_fd, BACKLOG);
  DCHECK(!status) << "Socket listen error.";
}

void recv_ack(int fd) {
  char ack[5];
  auto status = recv_all(fd, ack, 3);
  DCHECK(!status) << "socket recv error: ack, error code = " << errno;
  if (strcmp(ack, "OK") != 0) {
    LOG(FATAL) << "ack is wrong";
  }
}

void send_ack(int fd) {
  auto status = send_all(fd, "OK", 3);
  DCHECK(!status) << "socket send error: object ack";
}
