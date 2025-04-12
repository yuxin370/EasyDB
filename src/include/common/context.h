/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * context.h
 *
 * Identification: src/include/common/context.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2023 Renmin University of China
 */

#pragma once

#include <cstddef>
#include <cstdlib>
#include <nlohmann/json.hpp>
#include <vector>
#include "concurrency/lock_manager.h"
#include "recovery/log_manager.h"
#include "transaction/transaction.h"

using json = nlohmann::json;

namespace easydb {
// class TransactionManager;

// used for data_send
static int const_offset = -1;
/*
系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
*/
class Context {
 public:
  Context(LockManager *lock_mgr, LogManager *log_mgr, Transaction *txn, char *data_send = nullptr,
          int *offset = &const_offset)
      : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_(txn), data_send_(data_send), offset_(offset) {
    ellipsis_ = false;
  }

  // TransactionManager *txn_mgr_;
  LockManager *lock_mgr_;
  LogManager *log_mgr_;
  Transaction *txn_;
  char *data_send_;
  int *offset_;
  bool ellipsis_;
  json result_json;

  void InitJson() {
    SetJsonMsg("");
    InitJsonData();
  }

  void SetJsonMsg(const std::string &msg) { result_json["msg"] = msg; }
  void InitJsonData() {
    result_json["data"] = json::array();
    result_json["total"] = 0;
  }
  void AddJsonData(const std::vector<std::string> &row) {
    result_json["data"].emplace_back(row);
    // Update the total number of rows without the header row
    result_json["total"] = result_json["data"].size() - 1;
  }

  void PrintJsonMsg() { std::cout << result_json["msg"] << std::endl; }
  void PrintJson() { std::cout << result_json.dump(4) << std::endl; }

  int SerializeJsonTo(json &json, std::vector<char> &buf) {
    std::string json_str = json.dump(4);
    int len = json_str.length();
    buf.resize(len + 1);
    memcpy(buf.data(), json_str.c_str(), len);
    buf[len] = '\0';
    return len;
  }
  int SerializeTo(std::vector<char> &buf) { return SerializeJsonTo(result_json, buf); }

  int SerializeToWithLimit(std::vector<char> &buf, size_t max_size = 100) {
    auto &data_array = result_json["data"];
    // If the size of the "data" array(excluding the header row) is greater than max_size, slice it
    if (data_array.size() > max_size + 1) {
      // Create a new json object with a limited size of "data" array
      auto limited_json = json();
      limited_json["msg"] = result_json["msg"];
      limited_json["data"] = json::array();
      limited_json["total"] = result_json["total"];

      // Copy the first max_size rows(excluding the header row) to the new json object
      for (size_t i = 0; i < max_size + 1; ++i) {
        limited_json["data"].emplace_back(data_array[i]);
      }

      return SerializeJsonTo(limited_json, buf);
    }
    return SerializeTo(buf);
  }
};

}  // namespace easydb