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

/// Receive the next segment and append it into the stream.
template <typename T>
inline int stream_write(int conn_fd, T *stream) {
  TIMELINE("stream_write");
  int64_t receive_progress = 0;
  while (receive_progress < stream->Size()) {
    int status = stream_write_next<T>(conn_fd, stream, &receive_progress);
    if (status) {
      // return the error
      return status;
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
