#include "receiver.h"
#include "common/config.h"
#include "finegrained_pipelining.h"

#include "object_store.pb.h"
#include "util/protobuf_utils.h"

using objectstore::ObjectWriterRequest;
using objectstore::ReceiveObjectRequest;

Receiver::Receiver(ObjectStoreState &state, GlobalControlStoreClient &gcs_client, LocalStoreClient &local_store_client,
                   const std::string &my_address, int port)
    : state_(state), gcs_client_(gcs_client), my_address_(my_address), local_store_client_(local_store_client),
      pool_(HOPLITE_MAX_INFLOW_CONCURRENCY) {}

bool Receiver::check_and_store_inband_data(const ObjectID &object_id, int64_t object_size,
                                           const std::string &inband_data) {
  TIMELINE("Receiver::check_and_store_inband_data");
  if (inband_data.size() > 0) {
    LOG(DEBUG) << "fetching object directly from inband data";
    DCHECK(inband_data.size() <= inband_data_size_limit) << "unexpected inband data size";
    std::shared_ptr<Buffer> data;
    local_store_client_.Create(object_id, object_size, &data);
    data->CopyFrom(inband_data);
    local_store_client_.Seal(object_id);
    return true;
  }
  return false;
}

int Receiver::receive_object(const std::string &sender_ip, int sender_port, const ObjectID &object_id, Buffer *stream) {
  TIMELINE(std::string("Receiver::receive_object() ") + object_id.ToString());
  LOG(DEBUG) << "start receiving object " << object_id.ToString() << " from " << sender_ip
             << ", size = " << stream->Size() << ", intial_progress=" << stream->progress;
  int conn_fd;
  int ec = tcp_connect(sender_ip, sender_port, &conn_fd);
  if (ec) {
    LOG(ERROR) << "Failed to connect to sender (ip=" << sender_ip << ", port=" << sender_port << ").";
    return ec;
  }

  // send request
  ObjectWriterRequest req;
  auto ro_request = new ReceiveObjectRequest();
  ro_request->set_object_id(object_id.Binary());
  ro_request->set_object_size(stream->Size());
  ro_request->set_offset(stream->progress);
  req.set_allocated_receive_object(ro_request);
  SendProtobufMessage(conn_fd, req);

  // start receiving object
#ifdef HOPLITE_ENABLE_NONBLOCKING_SOCKET_RECV
  DCHECK(fcntl(conn_fd, F_SETFL, fcntl(conn_fd, F_GETFL) | O_NONBLOCK) >= 0)
      << "Cannot enable non-blocking for the socket (errno = " << errno << ").";
#endif
  ec = stream_receive<Buffer>(conn_fd, stream, stream->progress);
  LOG(DEBUG) << "receive " << object_id.ToString() << " done, error_code=" << ec;
  close(conn_fd);
  if (stream->IsFinished()) {
    gcs_client_.WriteLocation(object_id, my_address_, true, stream->Size(), stream->Data());
  }
  return ec;
}

void Receiver::pull_object(const ObjectID &object_id) {
  SyncReply reply = gcs_client_.GetLocationSync(object_id, true, my_address_);
  if (!check_and_store_inband_data(object_id, reply.object_size, reply.inband_data)) {
    // prepare object buffer for receiving.
    std::shared_ptr<Buffer> stream;
    auto pstatus = local_store_client_.GetBufferOrCreate(object_id, reply.object_size, &stream);
    DCHECK(pstatus.ok()) << "Plasma failed to allocate " << object_id.ToString() << " size = " << reply.object_size
                         << ", status = " << pstatus.ToString();
    LOG(DEBUG) << "Created buffer for " << object_id.ToString() << ", size=" << reply.object_size;
    std::string sender_ip = reply.sender_ip;
    // ---------------------------------------------------------------------------------------------
    // Here is our fault tolerance logic for multicast.
    // If the sender failed during sending the object, we retry with another sender that already has
    // the buffer ready before we declare that our stream is on progress. Retry with a random sender
    // could cause deadlock since the sender could also depend on us. In most cases we should get
    // the sender that was sending data to the failed sender, and we can continue transmission
    // immediately.
    // ---------------------------------------------------------------------------------------------
    while (!stream->IsFinished()) {
      LOG(DEBUG) << "Try receiving " << object_id.ToString() << " from " << sender_ip << ", size=" << reply.object_size;
      int ec = receive_object(sender_ip, HOPLITE_SENDER_PORT, object_id, stream.get());
      if (ec) {
        LOG(ERROR) << "Failed to receive " << object_id.ToString() << " from sender " << sender_ip;
        bool success = gcs_client_.HandlePullObjectFailure(object_id, my_address_, &sender_ip);
        if (!success) {
          LOG(ERROR) << "Cannot immediately recover from error. Retrying get location again...";
          reply = gcs_client_.GetLocationSync(object_id, true, my_address_);
          sender_ip = reply.sender_ip;
        }
        LOG(INFO) << "Retry receiving object from " << sender_ip;
      }
    }
    local_store_client_.Seal(object_id);
  }
}
