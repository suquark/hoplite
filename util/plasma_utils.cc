#include "plasma_utils.h"

unsigned char hex_to_dec(char a) {
  if (a <= '9') {
    return a - '0';
  } else {
    return a - 'a' + 10;
  }
}

plasma::ObjectID from_hex(char *hex) {
  unsigned char id[plasma::kUniqueIDSize];
  for (int i = 0; i < plasma::kUniqueIDSize; i++) {
    id[i] = hex_to_dec(hex[2 * i]) * 16 + hex_to_dec(hex[2 * i + 1]);
  }
  std::string binary = std::string((char *)id, plasma::kUniqueIDSize);

  return plasma::ObjectID::from_binary(binary);
}
