// Note: This header should only be included in the target soruce file.
#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <plasma/common.h>
#include <random>
#include <string>
#include <zlib.h>

#include "distributed_object_store.h"
#include "logging.h"
#include "object_store.grpc.pb.h"

using objectstore::IsReadyReply;
using objectstore::IsReadyRequest;
using objectstore::RegisterReply;
using objectstore::RegisterRequest;
using namespace plasma;

void register_group(const std::string &redis_address,
                    const int notification_port, const int num_of_nodes) {
  auto remote_address = redis_address + ":" + std::to_string(notification_port);
  auto channel =
      grpc::CreateChannel(remote_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<objectstore::NotificationServer::Stub> stub(
      objectstore::NotificationServer::NewStub(channel));
  grpc::ClientContext context;
  RegisterRequest request;
  RegisterReply reply;
  request.set_num_of_nodes(num_of_nodes);
  stub->Register(&context, request, &reply);
  DCHECK(reply.ok()) << "Group registeration failure!";
}

void is_ready(const std::string &redis_address, const int notification_port) {
  auto remote_address = redis_address + ":" + std::to_string(notification_port);
  auto channel =
      grpc::CreateChannel(remote_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<objectstore::NotificationServer::Stub> stub(
      objectstore::NotificationServer::NewStub(channel));
  grpc::ClientContext context;
  IsReadyRequest request;
  IsReadyReply reply;
  stub->IsReady(&context, request, &reply);
  DCHECK(reply.ok()) << "Synchronization failure!";
}

void barrier(const int rank, const std::string &redis_address,
             const int notification_port, const int num_of_nodes) {
  TIMELINE("barrier");
  if (rank == 0) {
    register_group(redis_address, notification_port, num_of_nodes);
  }
  is_ready(redis_address, notification_port);
}

uint32_t checksum_crc32(const std::shared_ptr<Buffer> &buffer) {
  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, buffer->data(), buffer->size());
  return crc;
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
      get_random_float_buffer(object_size / sizeof(T), object_id.hex());
  store.Put(buffer->data(), object_size, object_id);
  LOG(INFO) << "Object is created! object_id = " << object_id.hex()
            << ", size = " << object_size
            << ", the chosen random value: " << (*buffer)[1];
}

template <typename T>
void print_reduction_result(const ObjectID &object_id,
                            const std::shared_ptr<Buffer> &result,
                            T expected_sum) {
  const T *buffer = (const T *)result->data();
  int64_t num_elements = result->size() / sizeof(T);

  LOG(INFO) << "ObjectID(" << object_id.hex() << "), "
            << "CRC32 = " << checksum_crc32(result) << "\n"
            << "Results: [" << buffer[0] << ", " << buffer[1] << ", "
            << buffer[2] << ", " << buffer[3] << ", " << buffer[4] << ", ... , "
            << buffer[num_elements - 1] << "] \n"
            << "Result errors: first item = " << buffer[1] - expected_sum
            << ", last item = "
            << buffer[num_elements - 1] / (num_elements - 1) - expected_sum;
}

unsigned char _hex_to_dec(char a) {
  if (a <= '9') {
    return a - '0';
  } else {
    return a - 'a' + 10;
  }
}

ObjectID object_id_from_hex(const std::string &hex_str) {
  const char *hex = hex_str.c_str();
  unsigned char id[kUniqueIDSize];
  for (int i = 0; i < kUniqueIDSize; i++) {
    id[i] = _hex_to_dec(hex[2 * i]) * 16 + _hex_to_dec(hex[2 * i + 1]);
  }
  auto binary = std::string((char *)id, kUniqueIDSize);
  return ObjectID::from_binary(binary);
}

ObjectID object_id_from_suffix(std::string s) {
  s.insert(0, 40 - s.size(), '0');
  return object_id_from_hex(s);
}

ObjectID object_id_from_integer(int64_t num) {
  auto s = std::to_string(num);
  return object_id_from_suffix(s);
}

#endif // TEST_UTILS_H
