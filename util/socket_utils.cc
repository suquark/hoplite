#pragma once
#include <arpa/inet.h>
#include <chrono>
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


int send_all(int conn_fd, const void *buf, const size_t size) {
  size_t cursor = 0;
  while (cursor < size) {
    int bytes_sent = send(conn_fd, buf + cursor, size - cursor, 0);
    if (bytes_sent < 0) {
      LOG(ERROR) << "Socket send error (code=" << errno << ")";
      return bytes_sent;
    }
    cursor += bytes_sent;
  }
  return 0;
}

int recv_all(int conn_fd, void *buf, const size_t size) {
  size_t cursor = 0;
  while (cursor < size) {
    int bytes_recv = recv(conn_fd, buf + cursor, size - cursor, 0);
    if (bytes_recv < 0) {
      LOG(ERROR) << "Socket recv error (code=" << errno << ")";
      return bytes_recv;
    }
    cursor += bytes_recv;
  }
  return 0;
}


int tcp_connect(const std::string& ip_address, int port, int *conn_fd) {
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


