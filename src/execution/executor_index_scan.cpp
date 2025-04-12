/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_index_scan.cpp
 *
 * Identification: src/execution/executor_index_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "execution/executor_index_scan.h"
#include <cassert>
#include <cstdint>
#include <vector>
#include "type/type.h"
#include "type/type_id.h"

namespace easydb {

IndexScanExecutor::IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                                     std::vector<std::string> index_col_names, Context *context) {
  sm_manager_ = sm_manager;
  context_ = context;
  tab_name_ = std::move(tab_name);
  tab_ = sm_manager_->db_.get_table(tab_name_);
  conds_ = std::move(conds);
  // index_no_ = index_no;
  index_col_names_ = index_col_names;
  index_meta_ = *(tab_.get_index_meta(index_col_names_));
  fh_ = sm_manager_->fhs_.at(tab_name_).get();

  // cols_ = tab_.cols;
  schema_ = tab_.schema;
  len_ = schema_.GetInlinedStorageSize();
  // len_ = cols_.back().offset + cols_.back().len;
  std::map<CompOp, CompOp> swap_op = {
      {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
  };

  std::vector<std::string> cond_cols;
  std::unordered_set<std::string> cond_cols_set;
  for (auto &cond : conds_) {
    if (cond.lhs_col.tab_name != tab_name_) {
      // lhs is on other table, now rhs must be on this table
      assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
      // swap lhs and rhs
      std::swap(cond.lhs_col, cond.rhs_col);
      cond.op = swap_op.at(cond.op);
    }
    cond_cols.emplace_back(cond.lhs_col.col_name);
    cond_cols_set.emplace(cond.lhs_col.col_name);
  }
  // Now we just check columns of conditions must be the same as the index.
  // Actually, parts of columns can use the index.
  // TODO: support more complex conditions
  std::vector<std::string> cond_cols_vec(cond_cols_set.begin(), cond_cols_set.end());
  if (tab_.is_index(cond_cols_vec)) {
    fed_conds_ = conds_;
  }

  // lock table
  if (context_ != nullptr) {
    // context_->lock_mgr_->LockSharedOnTable(context_->txn_, fh_->GetFd());
    context_->lock_mgr_->LockISOnTable(context_->txn_, fh_->GetFd());

  }
}

void IndexScanExecutor::beginTuple() {
  // 找到索引的lower bound和upper bound (遍历cond)
  // 初始化scan_ 并扫描
  // 找到第一个满足条件的记录
  // std::cout << "IndexScanExecutor beginTuple" << std::endl;
  // 1. Determine the lower and upper bounds for the index scan based on the conditions
  auto index_name = sm_manager_->GetIxManager()->GetIndexName(tab_name_, index_col_names_);
  auto ih = sm_manager_->ihs_.at(index_name).get();
  Iid lower = ih->LeafBegin();
  Iid upper = ih->LeafEnd();
  // Initialize key buffer
  char *key_lower = new char[index_meta_.col_tot_len];
  char *key_upper = new char[index_meta_.col_tot_len];
  memset(key_lower, 0, index_meta_.col_tot_len);
  memset(key_upper, 0xFF, index_meta_.col_tot_len);
  // char *key = new char[index_meta_.col_tot_len];
  // memset(key, 0, index_meta_.col_tot_len);
  // Precompute offsets and lengths for each column in the index
  std::unordered_map<std::string, std::pair<int, int>> col_off_lens;
  int offset = 0;
  for (const auto &col : index_meta_.cols) {
    col_off_lens[col.name] = {offset, col.len};
    offset += col.len;
  }
  // Determine the bounds based on conditions
  for (const auto &cond : fed_conds_) {
    auto [offset, len] = col_off_lens[cond.lhs_col.col_name];
    // auto index_col = *tab_.get_col(cond.lhs_col.col_name);
    // memcpy(key + offset, cond.rhs_val.raw->data, len);
    switch (cond.op) {
      case OP_EQ:
        // lower = ih->lower_bound(key);
        // upper = ih->upper_bound(key);
        if (cond.rhs_val.GetTypeId() == TYPE_CHAR || cond.rhs_val.GetTypeId() == TYPE_VARCHAR) {
          memcpy(key_lower + offset, cond.rhs_val.GetData(), len);
          memcpy(key_upper + offset, cond.rhs_val.GetData(), len);
        } else {
          assert(uint32_t(len) == Type(cond.rhs_val.GetTypeId()).GetTypeSize(cond.rhs_val.GetTypeId()));
          cond.rhs_val.SerializeTo(key_lower + offset);
          cond.rhs_val.SerializeTo(key_upper + offset);
        }
        lower = ih->LowerBound(key_lower);
        upper = ih->UpperBound(key_upper);
        break;
      case OP_GE:
        // lower = ih->lower_bound(key);
        if (cond.rhs_val.GetTypeId() == TYPE_CHAR || cond.rhs_val.GetTypeId() == TYPE_VARCHAR) {
          memcpy(key_lower + offset, cond.rhs_val.GetData(), len);
        } else {
          assert(uint32_t(len) == Type(cond.rhs_val.GetTypeId()).GetTypeSize(cond.rhs_val.GetTypeId()));
          cond.rhs_val.SerializeTo(key_lower + offset);
        }
        lower = ih->LowerBound(key_lower);
        break;
      case OP_GT:
        // lower = ih->upper_bound(key);
        if (cond.rhs_val.GetTypeId() == TYPE_CHAR || cond.rhs_val.GetTypeId() == TYPE_VARCHAR) {
          memcpy(key_lower + offset, cond.rhs_val.GetData(), len);
        } else {
          assert(uint32_t(len) == Type(cond.rhs_val.GetTypeId()).GetTypeSize(cond.rhs_val.GetTypeId()));
          cond.rhs_val.SerializeTo(key_lower + offset);
        }
        lower = ih->UpperBound(key_lower);
        break;
      case OP_LE:
        // upper = ih->upper_bound(key);
        if (cond.rhs_val.GetTypeId() == TYPE_CHAR || cond.rhs_val.GetTypeId() == TYPE_VARCHAR) {
          memcpy(key_upper + offset, cond.rhs_val.GetData(), len);
        } else {
          assert(uint32_t(len) == Type(cond.rhs_val.GetTypeId()).GetTypeSize(cond.rhs_val.GetTypeId()));
          cond.rhs_val.SerializeTo(key_upper + offset);
        }
        upper = ih->UpperBound(key_upper);
        break;
      case OP_LT:
        // upper = ih->lower_bound(key);
        if (cond.rhs_val.GetTypeId() == TYPE_CHAR || cond.rhs_val.GetTypeId() == TYPE_VARCHAR) {
          memcpy(key_upper + offset, cond.rhs_val.GetData(), len);
        } else {
          assert(uint32_t(len) == Type(cond.rhs_val.GetTypeId()).GetTypeSize(cond.rhs_val.GetTypeId()));
          cond.rhs_val.SerializeTo(key_upper + offset);
        }
        upper = ih->LowerBound(key_upper);
        break;
      default:
        break;
    }
  }

  // 2. Initialize the index scan
  scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->GetBpm());

  // 3. Find the first tuple that satisfies the conditions
  while (!IsEnd()) {
    rid_ = scan_->GetRid();
    // Note: There maybe some not satisfied records in the index scan,
    // because the lower and upper bounds may not correct in some cases,
    // eg. when using a multiple-column index.
    if (predicate()) {
      break;
    }
    scan_->Next();
  }

  // Lock the gap between lower and upper bounds
  if (context_ != nullptr) {
    // The reason do before check IsEnd() is that we need to lock the gap of next key
    // when the current key is the last one in the index or the lower=upper.
    auto iid = scan_->GetIid();
    auto lower_iid = iid;
    context_->lock_mgr_->LockGapOnIndex(context_->txn_, iid, fh_->GetFd());
    // lock the gap of next key
    while (!IsEnd()) {
      scan_->Next();
      iid = scan_->GetIid();
      context_->lock_mgr_->LockGapOnIndex(context_->txn_, iid, fh_->GetFd());
    }
    // reset the lower bound to the first record
    scan_->set_lower(lower_iid);
  }

  delete[] key_lower;
  delete[] key_upper;
}

void IndexScanExecutor::nextTuple() {
  // TODO:
  // 使用scan_ 找到下一个满足条件的记录
  // std::cout << "IndexScanExecutor nextTuple" << std::endl;
  scan_->Next();
  // Note that scan_->next() may out of range
  while (!IsEnd()) {
    rid_ = scan_->GetRid();
    // Note: There maybe some not satisfied records in the index scan,
    // because the lower and upper bounds may not correct in some cases,
    // eg. when using a multiple-column index.
    if (predicate()) {
      break;
    }
    scan_->Next();
  }
}

// return true only all the conditions were true
bool IndexScanExecutor::predicate() {
  // std::cout << "IndexScanExecutor predicate" << std::endl;
  auto tuple = *this->Next();
  bool satisfy = true;
  // i.e. all conditions are connected with 'and' operator
  for (auto &cond : conds_) {
    // auto lhs_col = get_col(cols_, cond.lhs_col);
    Value lhs_v, rhs_v;
    lhs_v = tuple.GetValue(&schema_, cond.lhs_col.col_name);
    // lhs_v.DeserializeFrom(tuple.GetData(), &schema_, cond.lhs_col.col_name);
    if (cond.is_rhs_val) {
      rhs_v = cond.rhs_val;
    } else {
      rhs_v = tuple.GetValue(&schema_, cond.rhs_col.col_name);
      // rhs_v.DeserializeFrom(tuple.GetData(), &schema_, cond.rhs_col.col_name);
    }
    if (!cond.satisfy(lhs_v, rhs_v)) {
      satisfy = false;
      break;
    }
  }
  return satisfy;
}

}  // namespace easydb
