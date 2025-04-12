/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/config.h"
#include "storage/index/ix_defs.h"
#include "storage/table/tuple.h"

namespace easydb {

/* 标识事务状态 */
enum class TransactionState { DEFAULT, GROWING, SHRINKING, COMMITTED, ABORTED };

/* 系统的隔离级别，当前赛题中为可串行化隔离级别 */
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED, SERIALIZABLE };

/* 事务写操作类型，包括插入、删除、更新三种操作 */
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE, UPDATE_TUPLE };

/**
 * @brief 事务的写操作记录，用于事务的回滚
 * INSERT
 * --------------------------------
 * | wtype | tab_name | tuple_rid |
 * --------------------------------
 * DELETE / UPDATE
 * ----------------------------------------------
 * | wtype | tab_name | tuple_rid | tuple_value |
 * ----------------------------------------------
 */
class WriteRecord {
 public:
  WriteRecord() = default;

  // constructor for insert operation
  WriteRecord(WType wtype, const std::string &tab_name, const RID &rid)
      : wtype_(wtype), tab_name_(tab_name), rid_(rid) {}

  // constructor for delete & update operation
  WriteRecord(WType wtype, const std::string &tab_name, const RID &rid, const Tuple &tuple)
      : wtype_(wtype), tab_name_(tab_name), rid_(rid), tuple_(tuple) {}

  ~WriteRecord() = default;

  inline Tuple &GetTuple() { return tuple_; }

  inline RID &GetRid() { return rid_; }

  inline WType &GetWriteType() { return wtype_; }

  inline std::string &GetTableName() { return tab_name_; }

 private:
  WType wtype_;
  std::string tab_name_;
  RID rid_;
  Tuple tuple_;
};

/* 多粒度锁，加锁对象的类型，包括记录和表 */
enum class LockDataType { TABLE = 0, RECORD = 1, GAP = 2 };

/**
 * @description: 加锁对象的唯一标识
 */
class LockDataId {
 public:
  /* 表级锁 */
  LockDataId(int fd, LockDataType type) {
    assert(type == LockDataType::TABLE);
    fd_ = fd;
    type_ = type;
    // rid_.page_no = -1;
    // rid_.slot_no = -1;
    rid_.Set(0, 0);
  }

  /* 行级锁 */
  LockDataId(int fd, const RID &rid, LockDataType type) {
    assert(type == LockDataType::RECORD);
    fd_ = fd;
    rid_ = rid;
    type_ = type;
  }

  /* 间隙锁 */
  LockDataId(int fd, const Iid &iid, LockDataType type) {
    assert(type == LockDataType::GAP);
    fd_ = fd;
    rid_ = {iid.page_id_, iid.slot_num_};
    type_ = type;
  }

  // Note: If type_ is GAP, shifting the bits of type_ will result in overflow since 2 in binary is 10.
  // However, as RECORD type's binary representation is 1 and fd_ is different, it remains distinguishable
  // and avoids conflicts with TABLE type's binary representation.
  inline int64_t Get() const {
    if (type_ == LockDataType::TABLE) {
      // fd_
      return static_cast<int64_t>(fd_);
    } else {
      // fd_, rid_.page_no, rid.slot_no
      return ((static_cast<int64_t>(type_)) << 63) | ((static_cast<int64_t>(fd_)) << 31) |
             ((static_cast<int64_t>(rid_.GetPageId())) << 16) | rid_.GetSlotNum();
    }
  }

  bool operator==(const LockDataId &other) const {
    if (type_ != other.type_) return false;
    if (fd_ != other.fd_) return false;
    return rid_ == other.rid_;
  }
  int fd_;
  RID rid_;
  LockDataType type_;
};

/* 事务回滚原因 */
enum class AbortReason { LOCK_ON_SHIRINKING = 0, UPGRADE_CONFLICT, DEADLOCK_PREVENTION };

/* 事务回滚异常，在rmdb.cpp中进行处理 */
class TransactionAbortException : public std::exception {
  txn_id_t txn_id_;
  AbortReason abort_reason_;

 public:
  explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
      : txn_id_(txn_id), abort_reason_(abort_reason) {}

  txn_id_t GetTransactionId() { return txn_id_; }
  AbortReason GetAbortReason() { return abort_reason_; }
  std::string GetInfo() {
    switch (abort_reason_) {
      case AbortReason::LOCK_ON_SHIRINKING: {
        return "Transaction " + std::to_string(txn_id_) +
               " aborted because it cannot request locks on SHRINKING phase\n";
      } break;

      case AbortReason::UPGRADE_CONFLICT: {
        return "Transaction " + std::to_string(txn_id_) +
               " aborted because another transaction is waiting for upgrading\n";
      } break;

      case AbortReason::DEADLOCK_PREVENTION: {
        return "Transaction " + std::to_string(txn_id_) + " aborted for deadlock prevention\n";
      } break;

      default: {
        return "Transaction aborted\n";
      } break;
    }
  }
};

};  // namespace easydb

namespace std {

template <>
struct std::hash<easydb::LockDataId> {
  size_t operator()(const easydb::LockDataId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

}  // namespace std
