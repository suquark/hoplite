// This file should only be included in source files.

#pragma once

#include <cerrno>
#include <arpa/inet.h>
#include <fcntl.h> // for non-blocking socket
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/config.h"
#include "logging.h"
#include "socket_utils.h"

constexpr int64_t STREAM_MAX_BLOCK_SIZE = 4 * (2 << 20); // 4MB

template <typename T>
inline int stream_write_next(int conn_fd, T *stream,
                             int64_t *receive_progress) {
  int remaining_size = stream->Size() - *receive_progress;
  // here we receive no more than STREAM_MAX_BLOCK_SIZE for streaming
  int recv_block_size = remaining_size > STREAM_MAX_BLOCK_SIZE
                            ? STREAM_MAX_BLOCK_SIZE
                            : remaining_size;
  while (true) {
    int bytes_recv = recv(conn_fd, stream->MutableData() + *receive_progress,
                          recv_block_size, 0);
    if (bytes_recv < 0) {
      if (errno == EAGAIN) {
#ifndef HOPLITE_ENABLE_NONBLOCKING_SOCKET_RECV
        LOG(WARNING)
            << "[stream_write_next] socket recv error (EAGAIN). Ignored.";
#endif
        continue;
      }
      LOG(ERROR) << "[stream_write_next] socket recv error (" << strerror(errno)
                 << ", code=" << errno << ")";
      return -1;
    }
    *receive_progress += bytes_recv;
    return 0;
  }
}

template <typename T>
inline int stream_receive(int conn_fd, T *stream, int64_t offset=0) {
  TIMELINE("stream_receive");
  int64_t receive_progress = offset;
  while (receive_progress < stream->Size()) {
    int ec = stream_write_next<T>(conn_fd, stream, &receive_progress);
    if (ec) {
      // return the error
      LOG(ERROR) << "[stream_receive] socket receive error (" << strerror(errno)
                 << ", code=" << errno << ", receive_progress=" << receive_progress << ")";
      return ec;
    }
    // update the progress
#ifdef HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS
    stream->progress.store(receive_progress);
#else
    stream->progress = receive_progress;
#endif
  }
  return 0;
}

template <typename T>
inline int stream_send(int conn_fd, T *stream, int64_t offset=0) {
  TIMELINE("ObjectSender::stream_send()");
  LOG(DEBUG) << "ObjectSender::stream_send(), offset=" << offset;
  const uint8_t *data_ptr = stream->Data();
  const int64_t object_size = stream->Size();

  if (stream->IsFinished()) {
    int status = send_all(conn_fd, data_ptr + offset, object_size - offset);
    if (status) {
      LOG(ERROR) << "Failed to send object.";
      return status;
    }
    return 0;
  }
  int64_t cursor = offset;
  while (cursor < object_size) {
    int64_t current_progress = stream->progress;
    if (cursor < current_progress) {
      int bytes_sent =
          send(conn_fd, data_ptr + cursor, current_progress - cursor, 0);
      if (bytes_sent < 0) {
        LOG(ERROR) << "[stream_send] socket send error (" << strerror(errno)
                   << ", code=" << errno << ", cursor=" << cursor << ", stream_progress=" << current_progress << ")";
        if (errno == EAGAIN) {
          continue;
        }
        return errno;
      }
      cursor += bytes_sent;
    }
  }
  return 0;
}
