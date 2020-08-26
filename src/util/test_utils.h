// Note: This header should only be included in the target soruce file.
#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#include "common/buffer.h"
#include "common/id.h"
#include <chrono>
#include <random>
#include <string>
#include <thread>

#include "distributed_object_store.h"
#include "logging.h"
#include "object_store.grpc.pb.h"

using objectstore::IsReadyReply;
using objectstore::IsReadyRequest;
using objectstore::RegisterReply;
using objectstore::RegisterRequest;

void register_group(const std::string &redis_address,
                    const int notification_port, const int num_of_nodes) {
  TIMELINE("register_group");
  auto remote_address = redis_address + ":" + std::to_string(notification_port);
  auto channel =
      grpc::CreateChannel(remote_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<objectstore::NotificationServer::Stub> stub(
      objectstore::NotificationServer::NewStub(channel));
  grpc::ClientContext context;
  RegisterRequest request;
  RegisterReply reply;
  request.set_num_of_nodes(num_of_nodes);
  auto status = stub->Register(&context, request, &reply);
  DCHECK(status.ok()) << "Group registeration gRPC failure!";
  DCHECK(reply.ok()) << "Group registeration failure!";
}

void is_ready(const std::string &redis_address, const int notification_port,
              const std::string &my_address) {
  TIMELINE("is_ready");
  auto remote_address = redis_address + ":" + std::to_string(notification_port);
  auto channel =
      grpc::CreateChannel(remote_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<objectstore::NotificationServer::Stub> stub(
      objectstore::NotificationServer::NewStub(channel));
  grpc::ClientContext context;
  IsReadyRequest request;
  IsReadyReply reply;
  request.set_ip(my_address);
  auto status = stub->IsReady(&context, request, &reply);
  DCHECK(status.ok()) << "IsReady gRPC failure!";
  DCHECK(reply.ok()) << "Synchronization failure!";
}

void barrier(const int rank, const std::string &redis_address,
             const int notification_port, const int num_of_nodes,
             const std::string &my_address) {
  TIMELINE("barrier");
  if (rank == 0) {
    register_group(redis_address, notification_port, num_of_nodes);
  }
  is_ready(redis_address, notification_port, my_address);
}

void barrier_new(const std::string &redis_address, const int notification_port, const int num_of_nodes) {
  TIMELINE("barrier_new");
  auto remote_address = redis_address + ":" + std::to_string(notification_port);
  auto channel =
      grpc::CreateChannel(remote_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<objectstore::NotificationServer::Stub> stub(
      objectstore::NotificationServer::NewStub(channel));
  grpc::ClientContext context;
  BarrierRequest request;
  BarrierReply reply;
  request.set_num_of_nodes(num_of_nodes);
  auto status = stub->Barrier(&context, request, &reply);
  DCHECK(status.ok()) << "Barrier gRPC failure!";
}

float get_uniform_random_float(const std::string &seed_str) {
  std::seed_seq seed(seed_str.begin(), seed_str.end());
  std::default_random_engine eng{seed};
  std::uniform_real_distribution<float> dis(0, 1);
  return dis(eng);
}

std::shared_ptr<Buffer> get_random_float_buffer(size_t n_elements,
                                                const std::string &seed_str) {
  auto buf = std::make_shared<Buffer>(n_elements * sizeof(float));
  float *data = (float *)buf->MutableData();
  float random_number = get_uniform_random_float(seed_str);
  for (int64_t i = 0; i < n_elements; i++) {
    data[i] = i * random_number;
  }
  buf->Seal();
  return buf;
}

template <typename T>
void put_random_buffer(DistributedObjectStore &store, const ObjectID &object_id,
                       int64_t object_size) {
  DCHECK(object_size % sizeof(T) == 0);
  std::shared_ptr<Buffer> buffer =
      get_random_float_buffer(object_size / sizeof(T), object_id.Hex());
  store.Put(buffer, object_id);
  const T *view = (const T *)buffer->Data();
  LOG(INFO) << object_id.ToString() << " is created! "
            << "size = " << object_size
            << ", the chosen random value: " << view[1];
}

template <typename T>
void print_reduction_result(const ObjectID &object_id,
                            const std::shared_ptr<Buffer> &result,
                            T expected_sum) {
  const T *view = (const T *)result->Data();
  int64_t num_elements = result->Size() / sizeof(T);

  LOG(INFO) << object_id.ToString() << " CRC32 = " << result->CRC32() << "\n"
            << "Results: [" << view[0] << ", " << view[1] << ", " << view[2]
            << ", " << view[3] << ", " << view[4] << ", ... , "
            << view[num_elements - 1] << "] \n"
            << "Result errors: first item = " << view[1] - expected_sum
            << ", last item = "
            << view[num_elements - 1] / (num_elements - 1) - expected_sum;
}

ObjectID object_id_from_suffix(std::string s) {
  s.insert(0, 40 - s.size(), '0');
  return ObjectID::FromHex(s);
}

ObjectID object_id_from_integer(int64_t num) {
  auto s = std::to_string(num);
  return object_id_from_suffix(s);
}

std::thread timed_exit(int seconds) {
  std::this_thread::sleep_for(std::chrono::seconds(seconds));
  exit(0);
}

#endif // TEST_UTILS_H
