#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <plasma/common.h>
#include "logging.h"


enum class MessageType {
    ReceiveObject,
    ReceiveAndReduceObject,
};


ObjectID ReadObjectID(int conn_fd) {
  // receive object ID
  ObjectID object_id;
  auto status = recv_all(conn_fd, object_id.mutable_data(), object_id.size());
  DCHECK(!status) << "socket recv error: object id";
  return object_id;
}


ObjectWriterMessageType ReadMessageType(int conn_fd) {
  ObjectWriterMessageType msg_type;
  auto status = recv_all(conn_fd, &msg_type, sizeof(msg_type));
  DCHECK(!status) << "socket recv error: message type";
  return msg_type;
}

int64_t ReadObjectSize(int conn_fd) {
  // receive object size
  int64_t object_size;
  LOG(DEBUG) << "start receiving object " << object_id.hex() << " from "
             << incoming_ip;
  status = recv_all(conn_fd, &object_size, sizeof(object_size));
  DCHECK(!status) << "socket recv error: object size";
  LOG(DEBUG) << "Received object size = " << object_size;
  return object_size;
}





#endif // PROTOCOL_H