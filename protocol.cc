#include "protocol.h"
#include "logging.h"
#include "socket_utils.h"

using namespace plasma;

ObjectID ReadObjectID(int conn_fd) {
  // receive object ID
  ObjectID object_id;
  auto status = recv_all(conn_fd, object_id.mutable_data(), object_id.size());
  DCHECK(!status) << "socket recv error: object id";
  return object_id;
}

MessageType ReadMessageType(int conn_fd) {
  MessageType msg_type;
  auto status = recv_all(conn_fd, &msg_type, sizeof(msg_type));
  DCHECK(!status) << "socket recv error: message type";
  return msg_type;
}

int64_t ReadObjectSize(int conn_fd) {
  // receive object size
  int64_t object_size;
  auto status = recv_all(conn_fd, &object_size, sizeof(object_size));
  DCHECK(!status) << "socket recv error: object size";
  return object_size;
}
