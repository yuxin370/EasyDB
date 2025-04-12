/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_scan.h
 *
 * Identification: src/include/storage/index/ix_scan.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "record/rm_scan.h"
#include "storage/index/ix_defs.h"
#include "storage/index/ix_index_handle.h"
#include "record/rm_scan.h"
#include "common/rid.h"

namespace easydb {

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点
// TODO：对page遍历时，要加上读锁
class IxScan : public RecScan {
  const IxIndexHandle *ih_;
  Iid iid_;  // 初始为lower（用于遍历的指针）
  Iid end_;  // 初始为upper
  BufferPoolManager *bpm_;

 public:
  IxScan(const IxIndexHandle *ih, const Iid &lower, const Iid &upper, BufferPoolManager *bpm)
      : ih_(ih), iid_(lower), end_(upper), bpm_(bpm) {}

  void Next() override;

  bool IsEnd() const override { return iid_ == end_; }

  RID GetRid() const override;

  const Iid &GetIid() const { return iid_; }

  void set_lower(const Iid &lower) { iid_ = lower; }
};

}  // namespace easydb
