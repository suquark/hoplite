#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <plasma/common.h>
#include <plasma/client.h>
#include <atomic>

#include "global_control_store.h"
#include "logging.h"
#include "socket_utils.h"
#include "object_writer.h"

using namespace plasma;

// FIXME: here we assume we are downloading only 1 object
// need to fix this later
void *pending_write = NULL;
long pending_size = 0;
std::atomic_long progress;

void RunTCPServer(GlobalControlStoreClient& gcs_client, PlasmaClient& plasma_client, const std::string& ip, int port) {
  // data format:
  // [object_id (160bit), size (64bit), object]
  int server_fd, conn_fd;
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);
  int opt = 1;

  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  auto status = bind(server_fd, (struct sockaddr *)&address, sizeof(address));
  DCHECK(!status) << "Cannot bind to port " << port << ".";

  status = listen(server_fd, 10);
  DCHECK(!status) << "Socket listen error.";

  LOG(INFO) << "[TCPServer] tcp server is ready at " << ip << ":" << port;

  while (true) {
    char obj_id[kUniqueIDSize];
    long object_size;
    LOG(DEBUG) << "waiting for a connection";
    conn_fd = accept(server_fd, (struct sockaddr *)&address, &addrlen);
    char *incoming_ip = inet_ntoa(address.sin_addr);

    DCHECK(conn_fd >= 0) << "socket accept error";

    LOG(DEBUG) << "recieve a TCP connection from " << incoming_ip;

    auto status = recv_all(conn_fd, obj_id, kUniqueIDSize);
    DCHECK(!status) << "socket recv error: object id";

    ObjectID object_id = ObjectID::from_binary(obj_id);

    LOG(DEBUG) << "start receiving object " << object_id.hex() << " from "
               << incoming_ip;

    status = recv_all(conn_fd, &object_size, sizeof(object_size));
    DCHECK(!status) << "socket recv error: object size";

    LOG(DEBUG) << "Received object size = " << object_size;

    std::shared_ptr<Buffer> ptr;
    plasma_client.Create(object_id, object_size, NULL, 0, &ptr);

    progress = 0;
    pending_size = object_size;
    pending_write = ptr->mutable_data();
    gcs_client.write_object_location(object_id.hex(), ip);

    while (progress < object_size) {
      int bytes_recv = recv(conn_fd, ptr->mutable_data() + progress,
                            object_size - progress, 0);
      DCHECK(bytes_recv > 0) << "socket recv error: object content";
      progress += bytes_recv;
    }

    plasma_client.Seal(object_id);

    status = send_all(conn_fd, "OK", 3);
    DCHECK(!status) << "socket send error: object ack";

    close(conn_fd);
    LOG(INFO) << "[TCPServer] receiving object from " << incoming_ip
              << " completes";
  }
}
