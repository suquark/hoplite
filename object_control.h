#ifndef OBJECT_CONTROL_H
#define OBJECT_CONTROL_H

#include <plasma/common.h>

class GrpcServer {
public:
  GrpcServer(ObjectStoreState &state, const std::string &ip, int port);

  inline std::thread Run() { return std::thread(worker_loop, this); }

  bool PullObject(const std::string &remote_grpc_address,
                  const plasma::ObjectID &object_id);

private:
  void worker_loop();
};

#endif // OBJECT_CONTROL_H