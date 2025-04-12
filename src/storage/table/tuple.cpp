/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * tuple.cpp
 *
 * Identification: src/storage/table/tuple.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include <cassert>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "storage/table/tuple.h"

namespace easydb {

// TODO(Amadou): It does not look like nulls are supported. Add a null bitmap?
Tuple::Tuple(std::vector<Value> values, const Schema *schema) {
  assert(values.size() == schema->GetColumnCount());

  // 1. Calculate the size of the tuple.
  uint32_t tuple_size = schema->GetInlinedStorageSize();
  for (auto &i : schema->GetUnlinedColumns()) {
    auto len = values[i].GetStorageSize();
    if (len == EASYDB_VALUE_NULL) {
      len = 0;
    }
    tuple_size += sizeof(uint32_t) + len;
  }

  // 2. Allocate memory.
  data_.resize(tuple_size);
  std::fill(data_.begin(), data_.end(), 0);

  // 3. Serialize each attribute based on the input value.
  uint32_t column_count = schema->GetColumnCount();
  uint32_t offset = schema->GetInlinedStorageSize();

  for (uint32_t i = 0; i < column_count; i++) {
    const auto &col = schema->GetColumn(i);
    if (!col.IsInlined()) {
      // Serialize relative offset, where the actual varchar data is stored.
      *reinterpret_cast<uint32_t *>(data_.data() + col.GetOffset()) = offset;
      // Serialize varchar value, in place (size+data).
      values[i].SerializeTo(data_.data() + offset);
      auto len = values[i].GetStorageSize();
      if (len == EASYDB_VALUE_NULL) {
        len = 0;
      }
      offset += sizeof(uint32_t) + len;
    } else {
      values[i].SerializeTo(data_.data() + col.GetOffset());
    }
  }
}

Tuple::Tuple(std::vector<char> data) {
  // // 1. Calculate the size of the tuple.
  // uint32_t tuple_size = schema->GetInlinedStorageSize();
  // for (auto &i : schema->GetUnlinedColumns()) {
  //   auto len = values[i].GetStorageSize();
  //   if (len == EASYDB_VALUE_NULL) {
  //     len = 0;
  //   }
  //   tuple_size += sizeof(uint32_t) + len;
  // }

  // // 2. Allocate memory.
  // data_.resize(tuple_size);
  // std::fill(data_.begin(), data_.end(), 0);

  data_ = data;

  // // 3. Serialize each attribute based on the input value.
  // uint32_t column_count = schema->GetColumnCount();
  // uint32_t offset = schema->GetInlinedStorageSize();

  // for (uint32_t i = 0; i < column_count; i++) {
  //   const auto &col = schema->GetColumn(i);
  //   if (!col.IsInlined()) {
  //     // Serialize relative offset, where the actual varchar data is stored.
  //     *reinterpret_cast<uint32_t *>(data_.data() + col.GetOffset()) = offset;
  //     // Serialize varchar value, in place (size+data).
  //     values[i].SerializeTo(data_.data() + offset);
  //     auto len = values[i].GetStorageSize();
  //     if (len == EASYDB_VALUE_NULL) {
  //       len = 0;
  //     }
  //     offset += sizeof(uint32_t) + len;
  //   } else {
  //     values[i].SerializeTo(data_.data() + col.GetOffset());
  //   }
  // }
}

Tuple::Tuple(int size, char *data) {
  this->data_.resize(size);
  memcpy(this->data_.data(), data, size);
}

Tuple::Tuple(int size,const char *data) {
  this->data_.resize(size);
  memcpy(this->data_.data(), data, size);
}

auto Tuple::GetValue(const Schema *schema, const uint32_t column_idx) const -> Value {
  assert(schema);
  const TypeId column_type = schema->GetColumn(column_idx).GetType();
  const char *data_ptr = GetDataPtr(schema, column_idx);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

auto Tuple::GetValue(const Schema *schema, std::string column_name) const -> Value {
  assert(schema);
  const TypeId column_type = schema->GetColumn(column_name).GetType();
  const char *data_ptr = GetDataPtr(schema, column_name);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

auto Tuple::GetValue(const Column col) const -> Value {
  const TypeId column_type = col.GetType();
  const char *data_ptr = GetDataPtr(col);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

auto Tuple::GetValueVec(const Schema *schema) const -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.emplace_back(this->GetValue(schema, i));
  }
  return values;
}

auto Tuple::KeyFromTuple(const Schema &schema, const Schema &key_schema,
                         const std::vector<uint32_t> &key_attrs) const -> Tuple {
  std::vector<Value> values;
  values.reserve(key_attrs.size());
  for (auto idx : key_attrs) {
    values.emplace_back(this->GetValue(&schema, idx));
  }
  return {values, &key_schema};
}

auto Tuple::GetDataPtr(const Schema *schema, const uint32_t column_idx) const -> const char * {
  assert(schema);
  const auto &col = schema->GetColumn(column_idx);
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset = *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_.data() + offset);
}

auto Tuple::GetDataPtr(const Schema *schema, const std::string column_name) const -> const char * {
  assert(schema);
  const auto &col = schema->GetColumn(column_name);
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset = *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_.data() + offset);
}

auto Tuple::GetDataPtr(const Column col) const -> const char * {
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset = *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_.data() + offset);
}

auto Tuple::ToString(const Schema *schema) const -> std::string {
  std::stringstream os;

  int column_count = schema->GetColumnCount();
  bool first = true;
  os << "(";
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(schema, column_itr)) {
      os << "<NULL>";
    } else {
      Value val = (GetValue(schema, column_itr));
      os << val.ToString();
    }
  }
  os << ")";

  return os.str();
}

void Tuple::SerializeTo(char *storage) const {
  int32_t sz = data_.size();
  memcpy(storage, &sz, sizeof(int32_t));
  memcpy(storage + sizeof(int32_t), data_.data(), sz);
}

void Tuple::DeserializeFrom(const char *storage) {
  uint32_t size = *reinterpret_cast<const uint32_t *>(storage);
  this->data_.resize(size);
  memcpy(this->data_.data(), storage + sizeof(int32_t), size);
}

}  // namespace easydb
