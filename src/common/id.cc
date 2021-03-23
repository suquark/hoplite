#include <iostream>
#include "common/id.h"

std::string ObjectID::ToString() const {
  return std::string("ObjectID(") + Hex() + ")";
}

#define ID_OSTREAM_OPERATOR(id_type)                              \
  std::ostream &operator<<(std::ostream &os, const id_type &id) { \
    if (id.IsNil()) {                                             \
      os << "NIL_ID";                                             \
    } else {                                                      \
      os << id.Hex();                                             \
    }                                                             \
    return os;                                                    \
  }

ID_OSTREAM_OPERATOR(ObjectID);