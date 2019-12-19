#include <plasma/common.h>

unsigned char hex_to_dec(char a) {
  if (a <= '9') {
    return a - '0';
  } else {
    return a - 'a' + 10;
  }
}

ObjectID from_hex(char *hex) {
  unsigned char id[kUniqueIDSize];
  for (int i = 0; i < kUniqueIDSize; i++) {
    id[i] = hex_to_dec(hex[2 * i]) * 16 + hex_to_dec(hex[2 * i + 1]);
  }
  std::string binary = std::string((char *)id, kUniqueIDSize);

  ObjectID object_id = ObjectID::from_binary(binary);
  DCHECK(object_id.hex().compare(hex) == 0)
      << "error in decoding object id: object_id = " << object_id.hex()
      << "hex = " << hex;
  return object_id;
}