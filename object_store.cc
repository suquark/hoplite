#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include <plasma/client.h>
#include <plasma/common.h>
#include <plasma/test_util.h>
#include <unistd.h>

using namespace plasma;

double test(bool batched) {
  PlasmaClient client;
  client.Connect("/tmp/plasma", "");

  std::vector<ObjectID> object_ids;
  std::vector<std::string> data;
  std::vector<std::string> metadata;

  size_t n = 5;

  for (size_t i = 0; i < n; ++i) {
    object_ids.push_back(random_object_id());
    data.push_back("Hello");
    metadata.push_back("World");
  }

  auto start = std::chrono::system_clock::now();


  for (size_t i = 0; i < n; ++i) {
    client.CreateAndSeal(object_ids[i], data[i], metadata[i]);
  }

  auto end = std::chrono::system_clock::now();

  std::chrono::duration<double> diff = end - start;

  client.Disconnect();

  usleep(1000000);

  return diff.count();

}

int main() {
	return 0;
}

