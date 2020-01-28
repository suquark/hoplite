#ifndef ID_H
#define ID_H

#include <cinttypes>
#include <cstring>
#include <string>
#include <chrono>
#include <mutex>
#include <random>
#include <thread>
#include "util/logging.h"

constexpr int kUniqueIDSize = 20;

// Declaration.
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

/// A helper function to fill random bytes into the `data`.
/// Warning: this is not fork-safe, we need to re-seed after that.
template <typename T>
void FillRandom(T *data) {
  DCHECK(data != nullptr);
  auto randomly_seeded_mersenne_twister = []() {
    auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    // To increase the entropy, mix in a number of time samples instead of a single one.
    // This avoids the possibility of duplicate seeds for many workers that start in
    // close succession.
    // FIXME: This part of code is disabled due to its signicant impact on performance.
    // for (int i = 0; i < 128; i++) {
    //   std::this_thread::sleep_for(std::chrono::microseconds(10));
    //   seed += std::chrono::high_resolution_clock::now().time_since_epoch().count();
    // }
    std::mt19937 seeded_engine(seed);
    return seeded_engine;
  };

  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex random_engine_mutex;
  std::lock_guard<std::mutex> lock(random_engine_mutex);
  static std::mt19937 generator = randomly_seeded_mersenne_twister();
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (int i = 0; i < data->size(); i++) {
    (*data)[i] = static_cast<uint8_t>(dist(generator));
  }
}


// Change the compiler alignment to 1 byte (default is 8).
#pragma pack(push, 1)

template <typename T>
class BaseID {
 public:
  BaseID();
  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static T FromRandom();
  static T FromHex(const std::string &hex_str);

  static T FromBinary(const std::string &binary);
  static const T &Nil();
  static size_t Size() { return T::Size(); }

  size_t Hash() const;
  bool IsNil() const;
  bool operator==(const BaseID &rhs) const;
  bool operator!=(const BaseID &rhs) const;
  const uint8_t *Data() const;
  std::string Binary() const;
  std::string Hex() const;

 protected:
  BaseID(const std::string &binary) {
    std::memcpy(const_cast<uint8_t *>(this->Data()), binary.data(), T::Size());
  }
  // All IDs are immutable for hash evaluations. MutableData is only allow to use
  // in construction time, so this function is protected.
  uint8_t *MutableData();
  // For lazy evaluation, be careful to have one Id contained in another.
  // This hash code will be duplicated.
  mutable size_t hash_ = 0;
};


class ObjectID : public BaseID<ObjectID> {
 public:
  static size_t Size() { return kUniqueIDSize; }

  ObjectID() : BaseID() {}

  std::string ToString() const;

 protected:
  ObjectID(const std::string &binary);

 protected:
  uint8_t id_[kUniqueIDSize];
};


// Restore the compiler alignment to default (8 bytes).
#pragma pack(pop)


template <typename T>
BaseID<T>::BaseID() {
  // Using const_cast to directly change data is dangerous. The cached
  // hash may not be changed. This is used in construction time.
  std::fill_n(this->MutableData(), T::Size(), 0xff);
}

template <typename T>
T BaseID<T>::FromRandom() {
  std::string data(T::Size(), 0);
  FillRandom(&data);
  return T::FromBinary(data);
}

template <typename T>
T BaseID<T>::FromBinary(const std::string &binary) {
  DCHECK(binary.size() == T::Size())
      << "expected size is " << T::Size() << ", but got " << binary.size();
  T t = T::Nil();
  std::memcpy(t.MutableData(), binary.data(), T::Size());
  return t;
}

unsigned inline char __hex_to_dec(char a) {
  return (a <= '9') ? a - '0' : a - 'a' + 10;
}

template <typename T>
T BaseID<T>::FromHex(const std::string &hex_str) {
  DCHECK(hex_str.size() == T::Size() * 2)
      << "expected size is " << T::Size() * 2 << ", but got " << hex_str.size();
  
  T t = T::Nil();
  const char *hex = hex_str.c_str();
  uint8_t *id = t.MutableData();
  for (int i = 0; i < T::Size(); i++) {
    id[i] = __hex_to_dec(hex[2 * i]) * 16 + __hex_to_dec(hex[2 * i + 1]);
  }
  return t;
}

template <typename T>
const T &BaseID<T>::Nil() {
  static const T nil_id;
  return nil_id;
}

template <typename T>
bool BaseID<T>::IsNil() const {
  static T nil_id = T::Nil();
  return *this == nil_id;
}

template <typename T>
size_t BaseID<T>::Hash() const {
  // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
  // default value 0)
  if (!hash_) {
    hash_ = MurmurHash64A(Data(), T::Size(), 0);
  }
  return hash_;
}

template <typename T>
bool BaseID<T>::operator==(const BaseID &rhs) const {
  return std::memcmp(Data(), rhs.Data(), T::Size()) == 0;
}

template <typename T>
bool BaseID<T>::operator!=(const BaseID &rhs) const {
  return !(*this == rhs);
}

template <typename T>
uint8_t *BaseID<T>::MutableData() {
  return reinterpret_cast<uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
const uint8_t *BaseID<T>::Data() const {
  return reinterpret_cast<const uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
std::string BaseID<T>::Binary() const {
  return std::string(reinterpret_cast<const char *>(Data()), T::Size());
}

template <typename T>
std::string BaseID<T>::Hex() const {
  constexpr char hex[] = "0123456789abcdef";
  const uint8_t *id = Data();
  std::string result;
  for (int i = 0; i < T::Size(); i++) {
    unsigned int val = id[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

namespace std {

#define DEFINE_UNIQUE_ID(type)                                           \
  template <>                                                            \
  struct hash<type> {                                                    \
    size_t operator()(const type &id) const { return id.Hash(); }        \
  };                                                                     \
  template <>                                                            \
  struct hash<const type> {                                              \
    size_t operator()(const type &id) const { return id.Hash(); }        \
  };

DEFINE_UNIQUE_ID(ObjectID);
#undef DEFINE_UNIQUE_ID

}

#endif // ID_H