/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * value.cpp
 *
 * Identification: src/type/value.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include <cassert>
#include <string>
#include <utility>

#include "catalog/column.h"
#include "common/exception.h"
#include "type/type.h"
#include "type/value.h"
// #include "type/vector_type.h"

namespace easydb {

Value::Value(const Value &other) {
  type_id_ = other.type_id_;
  size_ = other.size_;
  manage_data_ = other.manage_data_;
  value_ = other.value_;
  switch (type_id_) {
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR:
      // case TypeId::TYPE_VECTOR:
      if (size_.len_ == EASYDB_VALUE_NULL) {
        value_.varlen_ = nullptr;
      } else {
        if (manage_data_) {
          value_.varlen_ = new char[size_.len_];
          memcpy(value_.varlen_, other.value_.varlen_, size_.len_);
        } else {
          value_ = other.value_;
        }
      }
      break;
    default:
      value_ = other.value_;
  }
}

auto Value::operator=(Value other) -> Value & {
  Swap(*this, other);
  return *this;
}

// BOOLEAN and TINYINT
Value::Value(TypeId type, int8_t i) : Value(type) {
  switch (type) {
    // case TypeId::TYPE_BOOLEAN:
    //   value_.boolean_ = i;
    //   size_.len_ = (value_.boolean_ == EASYDB_BOOLEAN_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    // case TypeId::TYPE_TINYINT:
    //   value_.tinyint_ = i;
    //   size_.len_ = (value_.tinyint_ == EASYDB_INT8_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    // case TypeId::TYPE_SMALLINT:
    //   value_.smallint_ = i;
    //   size_.len_ = (value_.smallint_ == EASYDB_INT16_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    case TypeId::TYPE_INT:
      value_.integer_ = i;
      size_.len_ = (value_.integer_ == EASYDB_INT32_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    case TypeId::TYPE_LONG:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == EASYDB_INT64_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for one-byte Value constructor");
  }
}

// SMALLINT
Value::Value(TypeId type, int16_t i) : Value(type) {
  switch (type) {
    // case TypeId::TYPE_BOOLEAN:
    //   value_.boolean_ = i;
    //   size_.len_ = (value_.boolean_ == EASYDB_BOOLEAN_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    // case TypeId::TYPE_TINYINT:
    //   value_.tinyint_ = i;
    //   size_.len_ = (value_.tinyint_ == EASYDB_INT8_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    // case TypeId::TYPE_SMALLINT:
    //   value_.smallint_ = i;
    //   size_.len_ = (value_.smallint_ == EASYDB_INT16_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    case TypeId::TYPE_INT:
      value_.integer_ = i;
      size_.len_ = (value_.integer_ == EASYDB_INT32_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    case TypeId::TYPE_LONG:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == EASYDB_INT64_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    case TypeId::TYPE_DATE:
      value_.timestamp_ = i;
      size_.len_ = (value_.timestamp_ == EASYDB_TIMESTAMP_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for two-byte Value constructor");
  }
}

// INTEGER
Value::Value(TypeId type, int32_t i) : Value(type) {
  switch (type) {
    // case TypeId::TYPE_BOOLEAN:
    //   value_.boolean_ = i;
    //   size_.len_ = (value_.boolean_ == EASYDB_BOOLEAN_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    // case TypeId::TYPE_TINYINT:
    //   value_.tinyint_ = i;
    //   size_.len_ = (value_.tinyint_ == EASYDB_INT8_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    // case TypeId::TYPE_SMALLINT:
    //   value_.smallint_ = i;
    //   size_.len_ = (value_.smallint_ == EASYDB_INT16_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    case TypeId::TYPE_INT:
      value_.integer_ = i;
      size_.len_ = (value_.integer_ == EASYDB_INT32_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    case TypeId::TYPE_LONG:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == EASYDB_INT64_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    case TypeId::TYPE_DATE:
      value_.timestamp_ = i;
      size_.len_ = (value_.timestamp_ == EASYDB_TIMESTAMP_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for integer_ Value constructor");
  }
}

// BIGINT and TIMESTAMP
Value::Value(TypeId type, int64_t i) : Value(type) {
  switch (type) {
    // case TypeId::TYPE_BOOLEAN:
    //   value_.boolean_ = i;
    //   size_.len_ = (value_.boolean_ == EASYDB_BOOLEAN_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    // case TypeId::TYPE_TINYINT:
    //   value_.tinyint_ = i;
    //   size_.len_ = (value_.tinyint_ == EASYDB_INT8_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    // case TypeId::TYPE_SMALLINT:
    //   value_.smallint_ = i;
    //   size_.len_ = (value_.smallint_ == EASYDB_INT16_NULL ? EASYDB_VALUE_NULL : 0);
    //   break;
    case TypeId::TYPE_INT:
      value_.integer_ = i;
      size_.len_ = (value_.integer_ == EASYDB_INT32_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    case TypeId::TYPE_LONG:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == EASYDB_INT64_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    case TypeId::TYPE_DATE:
      value_.timestamp_ = i;
      size_.len_ = (value_.timestamp_ == EASYDB_TIMESTAMP_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for eight-byte Value constructor");
  }
}

// BIGINT and TIMESTAMP
Value::Value(TypeId type, uint64_t i) : Value(type) {
  switch (type) {
    case TypeId::TYPE_LONG:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == EASYDB_INT64_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    case TypeId::TYPE_DATE:
      value_.timestamp_ = i;
      size_.len_ = (value_.timestamp_ == EASYDB_TIMESTAMP_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for timestamp_ Value constructor");
  }
}

// DECIMAL
Value::Value(TypeId type, double d) : Value(type) {
  switch (type) {
    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE:
      value_.decimal_ = d;
      size_.len_ = (value_.decimal_ == EASYDB_DECIMAL_NULL ? EASYDB_VALUE_NULL : 0);
      break;
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type for double Value constructor");
  }
}

// VARCHAR
Value::Value(TypeId type, const char *data, uint32_t len, bool manage_data) : Value(type) {
  switch (type) {
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR:
      // case TypeId::TYPE_VECTOR:
      if (data == nullptr) {
        value_.varlen_ = nullptr;
        size_.len_ = EASYDB_VALUE_NULL;
      } else {
        manage_data_ = manage_data;
        if (manage_data_) {
          assert(len < EASYDB_VARCHAR_MAX_LEN);
          value_.varlen_ = new char[len];
          assert(value_.varlen_ != nullptr);
          size_.len_ = len;
          memcpy(value_.varlen_, data, len);
        } else {
          // FUCK YOU GCC I do what I want.
          value_.const_varlen_ = data;
          size_.len_ = len;
        }
      }
      break;
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type  for variable-length Value constructor");
  }
}

Value::Value(TypeId type, const std::string &data) : Value(type) {
  switch (type) {
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR: {
      manage_data_ = true;
      // TODO(TAs): How to represent a null string here?
      uint32_t len = static_cast<uint32_t>(data.length()) + 1;
      value_.varlen_ = new char[len];
      assert(value_.varlen_ != nullptr);
      size_.len_ = len;
      memcpy(value_.varlen_, data.c_str(), len);
      break;
    }
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type  for variable-length Value constructor");
  }
}

Value::Value(TypeId type, const std::vector<double> &data) : Value(type) {
  switch (type) {
    // case TypeId::TYPE_VECTOR: {
    //   manage_data_ = true;
    //   auto len = data.size() * sizeof(double);
    //   value_.varlen_ = new char[len];
    //   assert(value_.varlen_ != nullptr);
    //   size_.len_ = len;
    //   memcpy(value_.varlen_, data.data(), len);
    //   break;
    // }
    default:
      throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid Type  for variable-length Value constructor");
  }
}

// delete allocated char array space
Value::~Value() {
  switch (type_id_) {
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR:
      // case TypeId::TYPE_VECTOR:
      if (manage_data_) {
        delete[] value_.varlen_;
      }
      break;
    default:
      break;
  }
}

auto Value::CheckComparable(const Value &o) const -> bool {
  switch (GetTypeId()) {
    // case TypeId::TYPE_BOOLEAN:
    //   return (o.GetTypeId() == TypeId::TYPE_BOOLEAN || o.GetTypeId() == TypeId::TYPE_VARCHAR);
    // case TypeId::TYPE_TINYINT:
    // case TypeId::TYPE_SMALLINT:
    case TypeId::TYPE_INT:
    case TypeId::TYPE_LONG:
    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE:
      switch (o.GetTypeId()) {
        // case TypeId::TYPE_TINYINT:
        // case TypeId::TYPE_SMALLINT:
        case TypeId::TYPE_INT:
        case TypeId::TYPE_LONG:
        // case TypeId::TYPE_DECIMAL:
        case TypeId::TYPE_FLOAT:
        case TypeId::TYPE_DOUBLE:
        case TypeId::TYPE_CHAR:
        case TypeId::TYPE_VARCHAR:
          return true;
        default:
          break;
      }  // SWITCH
      break;
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR:
      // Anything can be cast to a string!
      return true;
      break;
    default:
      break;
  }  // END OF SWITCH
  return false;
}

auto Value::CheckInteger() const -> bool {
  switch (GetTypeId()) {
    // case TypeId::TYPE_TINYINT:
    // case TypeId::TYPE_SMALLINT:
    case TypeId::TYPE_INT:
    case TypeId::TYPE_LONG:
      return true;
    default:
      break;
  }
  return false;
}

auto Value::GetColumn() const -> Column {
  switch (GetTypeId()) {
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR:
      // case TypeId::TYPE_VECTOR:
      { return Column{"<val>", GetTypeId(), GetStorageSize()}; }
    default:
      return Column{"<val>", GetTypeId()};
  }
}

auto Value::GetVector() const -> std::vector<double> {
  //   return reinterpret_cast<VectorType *>(Type::GetInstance(type_id_))->GetVector(*this);
}

}  // namespace easydb