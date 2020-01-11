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

float get_uniform_random_float(const std::string &seed_str) {
  std::seed_seq seed(seed_str.begin(), seed_str.end());
  std::default_random_engine eng{seed};
  std::uniform_real_distribution<float> dis(0, 1);
  return dis(eng);
}

std::unique_ptr<std::vector<float>>
get_random_float_buffer(size_t size, const std::string &seed_str) {
  std::unique_ptr<std::vector<float>> retval;
  auto buf = new std::vector<float>(size);
  float random_number = get_uniform_random_float(seed_str);
  for (int i = 0; i < size; i++) {
    (*buf)[i] = i * random_number;
  }
  retval.reset(buf);
  return retval;
}

template <typename T>
void put_random_buffer(DistributedObjectStore &store, const ObjectID &object_id,
                       int64_t object_size) {
  DCHECK(object_size % sizeof(T) == 0);
  std::unique_ptr<std::vector<T>> buffer =
      get_random_float_buffer(object_size / sizeof(T), object_id.Hex());
  store.Put(buffer->data(), object_size, object_id);
  LOG(INFO) << object_id.ToString() << " is created! "
            << "size = " << object_size
            << ", the chosen random value: " << (*buffer)[1];
}

template <typename T>
void print_reduction_result(const ObjectID &object_id,
                            const std::shared_ptr<Buffer> &result,
                            T expected_sum) {
  const T *buffer = (const T *)result->Data();
  int64_t num_elements = result->Size() / sizeof(T);

  LOG(INFO) << "ObjectID(" << object_id.Hex() << "), "
            << "CRC32 = " << result->CRC32() << "\n"
            << "Results: [" << buffer[0] << ", " << buffer[1] << ", "
            << buffer[2] << ", " << buffer[3] << ", " << buffer[4] << ", ... , "
            << buffer[num_elements - 1] << "] \n"
            << "Result errors: first item = " << buffer[1] - expected_sum
            << ", last item = "
            << buffer[num_elements - 1] / (num_elements - 1) - expected_sum;
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
