#pragma once

#include "util/logging.h"
#include "util/socket_utils.h"

template <typename T>
inline void SendProtobufMessage(int conn_fd, const T &message) {
  size_t message_size = message.ByteSizeLong();
  auto status = send_all(conn_fd, (void *)&message_size, sizeof(message_size));
  DCHECK(!status) << "socket send error: message_size";

  std::vector<uint8_t> message_buf(message_size);
  message.SerializeWithCachedSizesToArray(message_buf.data());

  status = send_all(conn_fd, (void *)message_buf.data(), message_buf.size());
  DCHECK(!status) << "socket send error: message";
}

template <typename T>
inline void ReceiveProtobufMessage(int conn_fd, T *message) {
  size_t message_len;
  int status = recv_all(conn_fd, &message_len, sizeof(message_len));
  DCHECK(!status) << "receive message_len failed";

  std::vector<uint8_t> message(message_len);
  status = recv_all(conn_fd, message.data(), message_len);
  DCHECK(!status) << "receive message failed";

  message->ParseFromArray(message.data(), message.size());
}
