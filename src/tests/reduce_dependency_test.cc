#include "object_directory/reduce_dependency.h"
#include <iostream>

int main() {
  std::cout << ReduceTreeChain(128, 8).DebugString();
  std::cout << ReduceTreeChain(152, 24).DebugString();
  std::cout << ReduceTreeChain(61, 2).DebugString();
  std::cout << ReduceTreeChain(32, 44).DebugString();
  return 0;
}
