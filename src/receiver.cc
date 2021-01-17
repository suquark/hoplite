#include "common/config.h"
#include "receiver.h"
#include "finegrained_pipelining.h"

#include "object_store.pb.h"
#include "util/protobuf_utils.h"

using objectstore::ObjectWriterRequest;
using objectstore::ReceiveObjectRequest;

Receiver::Receiver(ObjectStoreState &state,
                   GlobalControlStoreClient &gcs_client,
                   LocalStoreClient &local_store_client,
                   const std::string &server_ipaddr, int port)
    : state_(state), gcs_client_(gcs_client), server_ipaddr_(server_ipaddr),
      local_store_client_(local_store_client),
      pool_(HOPLITE_MAX_INFLOW_CONCURRENCY) {

bool Receiver::check_and_store_inband_data(
    const ObjectID &object_id, int64_t object_size,
    const std::string &inband_data) {
  TIMELINE("Receiver::check_and_store_inband_data");
  if (inband_data.size() > 0) {
    LOG(DEBUG) << "fetching object directly from inband data";
    DCHECK(inband_data.size() <= inband_data_size_limit)
        << "unexpected inband data size";
    std::shared_ptr<Buffer> data;
    local_store_client_.Create(object_id, object_size, &data);
    data->CopyFrom(inband_data);
    local_store_client_.Seal(object_id);
    return true;
  }
  return false;
}

int Receiver::receive_object(
    const std::string &sender_ip, int sender_port, const ObjectID &object_id, Buffer *stream) {
  TIMELINE(std::string("Receiver::receive_object() ") + object_id.ToString() + " " + std::to_string(object_size));    
  LOG(DEBUG) << "start receiving object " << object_id.ToString() << ", size = " << object_size;
  int conn_fd;
  int ec = tcp_connect(sender_ip, sender_port, &conn_fd);
  if (ec) {
    LOG(ERROR) << "Failed to connect to sender (ip=" << sender_ip << ", port=" << sender_port << ").";
    return ec;
  }

  // send request
  ObjectWriterRequest req;
  auto ro_request = new ReceiveObjectRequest();
  ro_request->set_object_id(object_id->Binary());
  ro_request->set_offset(stream->progress);
  req.set_allocated_receive_object(ro_request);
  SendProtobufMessage(conn_fd, req);

  // start receiving object
  int ec = stream_write<Buffer>(conn_fd, stream);
  if (!ec) {
#ifdef HOPLITE_ENABLE_ACK
    // TODO: handle error here.
    send_ack(conn_fd);
#endif
    LOG(DEBUG) << object_id.ToString() << " received";
  }
  close(conn_fd);
  return ec;
}

void Receiver::pull_object(const ObjectID &object_id) {
  SyncReply reply = gcs_client_.GetLocationSync(object_id, true);
  if (!check_and_store_inband_data(object_id, reply.object_size, reply.inband_data)) {
    // prepare object buffer for receiving.
    std::shared_ptr<Buffer> stream;
    auto pstatus = local_store_client_.Create(object_id, reply.object_size, &stream);
    DCHECK(pstatus.ok()) << "Plasma failed to allocate " << object_id.ToString()
                          << " size = " << reply.object_size
                          << ", status = " << pstatus.ToString();
    // notify other nodes that our stream is on progress
    gcs_client_.WriteLocation(object_id, server_ipaddr_, false, reply.object_size);
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
      int status = receive_object(reply.sender_ip, HOPLITE_SENDER_PORT, object_id, stream.get());
      if (status) {
        LOG(ERROR) << "Failed to receive object " << object_id << " from sender " << reply.sender_ip; 
      }
      // TODO(siyuan): Change the sender.
    }
    local_store_client_.Seal(object_id);
  }
}
