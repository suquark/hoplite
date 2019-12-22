#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <plasma/common.h>

enum class MessageType {
  ReceiveObject,
  ReceiveAndReduceObject,
};

plasma::ObjectID ReadObjectID(int conn_fd);

MessageType ReadMessageType(int conn_fd);

void SendMessageType(int conn_fd, MessageType msg_type);

int64_t ReadObjectSize(int conn_fd);

#endif // PROTOCOL_H
