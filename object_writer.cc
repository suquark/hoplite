#include <cstdint>
#include <csignal>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <plasma/client.h>
#include <plasma/common.h>

#include "global_control_store.h"
#include "logging.h"
#include "object_writer.h"
#include "socket_utils.h"

using namespace plasma;

TCPServer::TCPServer(ObjectStoreState &state,
                     GlobalControlStoreClient &gcs_client,
                     PlasmaClient &plasma_client,
                     const std::string &server_ipaddr, int port)
    : state_(state), gcs_client_(gcs_client), server_ipaddr_(server_ipaddr),
      plasma_client_(plasma_client) {
  tcp_bind_and_listen(port, &address_, &server_fd_);
  LOG(INFO) << "[TCPServer] tcp server is ready at " << server_ipaddr << ":"
            << port;
}

void TCPServer::worker_loop() {
  while (true) {
    recv_object();
  }
}

void TCPServer::recv_object() {
  // Protocol: [object_id(kUniqueIDSize), object_size(8B), buffer(*)]

  LOG(DEBUG) << "waiting for a connection";
  socklen_t addrlen = sizeof(address_);
  int conn_fd = accept(server_fd_, (struct sockaddr *)&address_, &addrlen);
  DCHECK(conn_fd >= 0) << "socket accept error";
  char *incoming_ip = inet_ntoa(address_.sin_addr);
  LOG(DEBUG) << "recieve a TCP connection from " << incoming_ip;

  // receive object ID
  char obj_id[kUniqueIDSize];
  auto status = recv_all(conn_fd, obj_id, kUniqueIDSize);
  DCHECK(!status) << "socket recv error: object id";
  ObjectID object_id = ObjectID::from_binary(obj_id);

  // receive object size
  int64_t object_size;
  LOG(DEBUG) << "start receiving object " << object_id.hex() << " from "
             << incoming_ip;
  status = recv_all(conn_fd, &object_size, sizeof(object_size));
  DCHECK(!status) << "socket recv error: object size";
  LOG(DEBUG) << "Received object size = " << object_size;

  // receive object buffer
  std::shared_ptr<Buffer> ptr;
  plasma_client_.Create(object_id, object_size, NULL, 0, &ptr);
  state_.progress = 0;
  state_.pending_size = object_size;
  state_.pending_write = ptr->mutable_data();
  gcs_client_.write_object_location(object_id.hex(), server_ipaddr_);
  while (state_.progress < object_size) {
    int bytes_recv = recv(conn_fd, ptr->mutable_data() + state_.progress,
                          object_size - state_.progress, 0);
    DCHECK(bytes_recv > 0) << "socket recv error: object content";
    state_.progress += bytes_recv;
  }
  plasma_client_.Seal(object_id);

  // reply message
  status = send_all(conn_fd, "OK", 3);
  DCHECK(!status) << "socket send error: object ack";

  close(conn_fd);
  LOG(INFO) << "[TCPServer] receiving object from " << incoming_ip
            << " completes";
}
