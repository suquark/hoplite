#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <queue>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/id.h"
#include "logging.h"
#include "object_store.grpc.pb.h"

using objectstore::GetLocationAsyncAnswerReply;
using objectstore::GetLocationAsyncAnswerRequest;
using objectstore::GetLocationAsyncReply;
using objectstore::GetLocationAsyncRequest;
using objectstore::GetLocationSyncReply;
using objectstore::GetLocationSyncRequest;
using objectstore::WriteLocationReply;
using objectstore::WriteLocationRequest;

class NotificationListenerImpl final
    : public objectstore::NotificationListener::Service {
public:
  NotificationListenerImpl() : objectstore::NotificationListener::Service() {}

  grpc::Status
  GetLocationAsyncAnswer(grpc::ServerContext *context,
                         const GetLocationAsyncAnswerRequest *request,
                         GetLocationAsyncAnswerReply *reply) {
    for (auto &object : request->objects()) {
      ObjectID object_id = ObjectID::FromBinary(object.object_id());
      std::string sender_ip = object.sender_ip();
      std::string query_id = object.query_id();
      size_t object_size = object.object_size();
      LOG(INFO) << "[NotificationListener] [GetLocationAsyncAnswer] ID: "
                << object_id.Hex() << " IP: " << sender_ip
                << " Query: " << query_id << " Size: " << object_size;
      reply->set_ok(true);
    }
    return grpc::Status::OK;
  }
};

class NotificationListener {
public:
  NotificationListener(const std::string &my_address,
                       const int notification_port)
      : notification_port_(notification_port),
        service_(std::make_shared<NotificationListenerImpl>()) {
    std::string grpc_address =
        my_address + ":" + std::to_string(notification_port);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&*service_);
    grpc_server_ = builder.BuildAndStart();
  }
  inline std::thread Run() {
    std::thread notification_thread(&NotificationListener::worker_loop, this);
    return notification_thread;
  }

private:
  void worker_loop() {
    LOG(INFO) << "[NotificationListener] notification listener started";
    grpc_server_->Wait();
  }

  const int notification_port_;

  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<NotificationListenerImpl> service_;
};

std::shared_ptr<grpc::Channel> channel;
std::unique_ptr<objectstore::NotificationServer::Stub> stub;

void write_location(const ObjectID &object_id, const std::string &sender_ip,
                    size_t object_size) {
  TIMELINE("write_location");
  LOG(INFO) << "Adding object " << object_id.Hex()
            << " to notification server with address = " << sender_ip;
  grpc::ClientContext context;
  WriteLocationRequest request;
  WriteLocationReply reply;
  request.set_object_id(object_id.Binary());
  request.set_sender_ip(sender_ip);
  request.set_finished(true);
  request.set_object_size(object_size);
  stub->WriteLocation(&context, request, &reply);
  DCHECK(reply.ok()) << "WriteObjectLocation for " << object_id.ToString()
                     << " failed.";
}

void getlocationasync(const ObjectID &object_id, const std::string &receiver_ip,
                      const std::string &query_id, bool occupying) {
  TIMELINE("getlocationasync");
  LOG(INFO) << "Async get location of " << object_id.Hex();
  grpc::ClientContext context;
  GetLocationAsyncRequest request;
  GetLocationAsyncReply reply;
  request.add_object_ids(object_id.Binary());
  request.set_receiver_ip(receiver_ip);
  request.set_query_id(query_id);
  request.set_occupying(occupying);
  stub->GetLocationAsync(&context, request, &reply);
  DCHECK(reply.ok()) << "getlocationasync for " << object_id.ToString()
                     << " failed.";
}

void getlocationsync(const ObjectID &object_id, bool occupying) {
  TIMELINE("getlocationsync");
  LOG(INFO) << "Sync get location of " << object_id.Hex();
  grpc::ClientContext context;
  GetLocationSyncRequest request;
  GetLocationSyncReply reply;
  request.set_object_id(object_id.Binary());
  request.set_occupying(occupying);
  stub->GetLocationSync(&context, request, &reply);
  LOG(INFO) << "getlocationsync reply: " << reply.sender_ip()
            << " Size: " << reply.object_size();
}

void TEST1() {
  LOG(INFO) << "=========== TEST1 ===========";
  ObjectID object_id = ObjectID::FromRandom();
  std::string sender_ip = "1.2.3.4";
  size_t object_size = 100;
  LOG(INFO) << "object_id: " << object_id.Hex() << " sender_ip: " << sender_ip;
  write_location(object_id, sender_ip, object_size);
  getlocationsync(object_id, true);
}

void TEST2(const std::string &my_address) {
  LOG(INFO) << "=========== TEST2 ===========";
  ObjectID object_id = ObjectID::FromRandom();
  std::string sender_ip = "1.2.3.4";
  size_t object_size = 200;
  LOG(INFO) << "object_id: " << object_id.Hex() << " sender_ip: " << sender_ip;
  write_location(object_id, sender_ip, object_size);
  getlocationasync(object_id, my_address, "TEST2_query_id", true);
}

void TEST3(const std::string &my_address) {
  LOG(INFO) << "=========== TEST3 ===========";
  ObjectID object_id = ObjectID::FromRandom();
  std::string sender_ip_1 = "1.2.3.4";
  std::string sender_ip_2 = "2.3.4.5";
  size_t object_size = 300;
  LOG(INFO) << "object_id: " << object_id.Hex()
            << " sender_ip_1: " << sender_ip_1
            << " sender_ip_2: " << sender_ip_2;
  write_location(object_id, sender_ip_1, object_size);
  write_location(object_id, sender_ip_2, object_size);
  getlocationasync(object_id, my_address, "TEST3_query_id", true);
  getlocationsync(object_id, true);
}

void TEST4(const std::string &my_address) {
  LOG(INFO) << "=========== TEST4 ===========";
  ObjectID object_id = ObjectID::FromRandom();
  std::string sender_ip_1 = "1.2.3.4";
  std::string sender_ip_2 = "2.3.4.5";
  size_t object_size = 400;
  LOG(INFO) << "object_id: " << object_id.Hex()
            << " sender_ip_1: " << sender_ip_1
            << " sender_ip_2: " << sender_ip_2;
  getlocationasync(object_id, my_address, "TEST4_query_id", true);
  write_location(object_id, sender_ip_1, object_size);
  write_location(object_id, sender_ip_2, object_size);
  getlocationsync(object_id, true);
}

void TEST5(const std::string &my_address) {
  LOG(INFO) << "=========== TEST5 ===========";
  ObjectID object_id = ObjectID::FromRandom();
  std::string sender_ip_1 = "1.2.3.4";
  std::string sender_ip_2 = "2.3.4.5";
  size_t object_size = 300;
  LOG(INFO) << "object_id: " << object_id.Hex()
            << " sender_ip_1: " << sender_ip_1
            << " sender_ip_2: " << sender_ip_2;
  write_location(object_id, sender_ip_1, object_size);
  write_location(object_id, sender_ip_2, object_size);
  getlocationasync(object_id, my_address, "TEST3_query_id1", false);
  getlocationasync(object_id, my_address, "TEST3_query_id2", true);
  getlocationsync(object_id, true);
}

double average(const std::vector<double> &data) {
  double sum = 0;
  for (auto d : data) {
    sum += d;
  }
  return sum / data.size();
}

double standard_deviation(const std::vector<double> &data) {
  double avg = average(data);
  double sum = 0;
  for (auto d : data) {
    sum += (d - avg) * (d - avg);
  }
  return sqrt(sum / data.size());
}

void SPEED_TEST(size_t object_size) {
  LOG(INFO) << "=========== SPEED TEST1 ===========";
  ObjectID object_id = ObjectID::FromRandom();
  std::string sender_ip = "1.2.3.4";
  LOG(INFO) << "object_id: " << object_id.Hex() << " sender_ip: " << sender_ip;
  int num_tests = 100;
  std::vector<double> write_location_latencies;
  std::vector<double> get_location_latencies;
  for (int i = 0; i < num_tests; i++) {
    auto start = std::chrono::system_clock::now();
    write_location(object_id, sender_ip, object_size);
    auto after_writelocation = std::chrono::system_clock::now();
    getlocationsync(object_id, true);
    auto after_getlocation = std::chrono::system_clock::now();
    std::chrono::duration<double> diff_writelocation =
        after_writelocation - start;
    std::chrono::duration<double> diff_getlocation =
        after_getlocation - after_writelocation;
    write_location_latencies.push_back(diff_writelocation.count());
    get_location_latencies.push_back(diff_getlocation.count());
  }

  LOG(INFO) << "write location latency = " << average(write_location_latencies)
            << ","
            << " standard deviation = "
            << standard_deviation(write_location_latencies);

  LOG(INFO) << "get location latency = " << average(get_location_latencies)
            << ","
            << " standard deviation = "
            << standard_deviation(get_location_latencies);
}

int main(int argc, char **argv) {
  std::string notification_address = std::string(argv[1]);
  std::string my_address = std::string(argv[2]);
  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::INFO);
  std::unique_ptr<NotificationListener> notification_listener;
  std::thread notification_listener_thread;
  notification_listener.reset(new NotificationListener(my_address, 8888));
  notification_listener_thread = notification_listener->Run();
  channel = grpc::CreateChannel(notification_address + ":7777",
                                grpc::InsecureChannelCredentials());
  stub = objectstore::NotificationServer::NewStub(channel);
  TEST1();
  TEST2(my_address);
  TEST3(my_address);
  TEST4(my_address);
  TEST5(my_address);
  SPEED_TEST(1);
  SPEED_TEST(100);
  SPEED_TEST(4095);
  SPEED_TEST(1024 * 1024 * 1024);
  notification_listener_thread.join();
  return 0;
}
