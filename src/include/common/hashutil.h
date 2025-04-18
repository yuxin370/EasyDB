#pragma once

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <string>

#include "type/value.h"

namespace easydb {

using hash_t = std::size_t;

class HashUtil {
 private:
  static const hash_t PRIME_FACTOR = 10000019;

 public:
  static inline auto HashBytes(const char *bytes, size_t length) -> hash_t {
    // https://github.com/greenplum-db/gpos/blob/b53c1acd6285de94044ff91fbee91589543feba1/libgpos/src/utils.cpp#L126
    hash_t hash = length;
    for (size_t i = 0; i < length; ++i) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ bytes[i];
    }
    return hash;
  }

  static inline auto CombineHashes(hash_t l, hash_t r) -> hash_t {
    hash_t both[2] = {};
    both[0] = l;
    both[1] = r;
    return HashBytes(reinterpret_cast<char *>(both), sizeof(hash_t) * 2);
  }

  static inline auto SumHashes(hash_t l, hash_t r) -> hash_t {
    return (l % PRIME_FACTOR + r % PRIME_FACTOR) % PRIME_FACTOR;
  }

  template <typename T>
  static inline auto Hash(const T *ptr) -> hash_t {
    return HashBytes(reinterpret_cast<const char *>(ptr), sizeof(T));
  }

  template <typename T>
  static inline auto HashPtr(const T *ptr) -> hash_t {
    return HashBytes(reinterpret_cast<const char *>(&ptr), sizeof(void *));
  }

  /** @return the hash of the value */
  static inline auto HashValue(const Value *val) -> hash_t {
    switch (val->GetTypeId()) {
    //   case TypeId::TINYINT: {
    //     auto raw = static_cast<int64_t>(val->GetAs<int8_t>());
    //     return Hash<int64_t>(&raw);
    //   }
    //   case TypeId::SMALLINT: {
    //     auto raw = static_cast<int64_t>(val->GetAs<int16_t>());
    //     return Hash<int64_t>(&raw);
    //   }
      case TypeId::TYPE_INT: {
        auto raw = static_cast<int64_t>(val->GetAs<int32_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::TYPE_LONG: {
        auto raw = static_cast<int64_t>(val->GetAs<int64_t>());
        return Hash<int64_t>(&raw);
      }
    //   case TypeId::BOOLEAN: {
    //     auto raw = val->GetAs<bool>();
    //     return Hash<bool>(&raw);
    //   }
      case TypeId::TYPE_FLOAT: {
        auto raw = val->GetAs<double>();
        return Hash<double>(&raw);
      }
        case TypeId::TYPE_DOUBLE: {
        auto raw = val->GetAs<double>();
        return Hash<double>(&raw);
      }
      case TypeId::TYPE_VARCHAR: {
        auto raw = val->GetData();
        auto len = val->GetStorageSize();
        return HashBytes(raw, len);
      }
      case TypeId::TYPE_DATE: {
        auto raw = val->GetAs<uint64_t>();
        return Hash<uint64_t>(&raw);
      }
      default: {
        EASYDB_ASSERT(false, "Unsupported type.");
      }
    }
  }
};

}  // namespace easydb
