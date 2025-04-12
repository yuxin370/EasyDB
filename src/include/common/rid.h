/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rid.h
 *
 * Identification: src/include/common/rid.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cstdint>
#include <sstream>
#include <string>

#include "common/config.h"

namespace easydb {

class RID {
 public:
  /** The default constructor creates an invalid RID! */
  RID() = default;

  /**
   * Creates a new Record Identifier for the given page identifier and slot number.
   * @param page_no page identifier
   * @param slot_no slot number
   */
  RID(page_id_t page_id, slot_id_t slot_num) : page_id_(page_id), slot_num_(slot_num) {}

  explicit RID(int64_t rid) : page_id_(static_cast<page_id_t>(rid >> 32)), slot_num_(static_cast<slot_id_t>(rid)) {}

  inline auto Get() const -> int64_t { return (static_cast<int64_t>(page_id_)) << 32 | slot_num_; }

  inline auto GetPageId() const -> page_id_t { return page_id_; }

  inline auto GetSlotNum() const -> slot_id_t { return slot_num_; }

  inline void Set(page_id_t page_id, slot_id_t slot_num) {
    page_id_ = page_id;
    slot_num_ = slot_num;
  }

  inline void SetPageId(page_id_t page_id) { page_id_ = page_id; }

  inline void SetSlotNum(slot_id_t slot_num) { slot_num_ = slot_num; }

  inline auto ToString() const -> std::string {
    std::stringstream os;
    // os << "page_id: " << page_id_;
    // os << " slot_num: " << slot_num_ << "\n";
    os << "(" << page_id_ << "," << slot_num_ << ")";
    return os.str();
  }

  friend auto operator<<(std::ostream &os, const RID &rid) -> std::ostream & {
    os << rid.ToString();
    return os;
  }

  auto operator==(const RID &other) const -> bool { return page_id_ == other.page_id_ && slot_num_ == other.slot_num_; }

 private:
  page_id_t page_id_{INVALID_PAGE_ID};
  uint32_t slot_num_{0};  // logical offset from 0, 1...
};

}  // namespace easydb

namespace std {
template <>
struct hash<easydb::RID> {
  auto operator()(const easydb::RID &obj) const -> size_t { return hash<int64_t>()(obj.Get()); }
};
}  // namespace std
