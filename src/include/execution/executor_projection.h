/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <functional>
#include "common/errors.h"
#include "defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"

namespace easydb {

class ProjectionExecutor : public AbstractExecutor {
 private:
  std::unique_ptr<AbstractExecutor> prev_;  // 投影节点的儿子节点
  // std::string tab_name_;
  // std::vector<ColMeta> cols_;               // 需要投影的字段
  size_t len_;                     // 字段总长度
  std::vector<uint32_t> sel_ids_;  // 投影字段对应的id(位置)

  // RmRecord projection_records_;             // temp projection record(added by flerovium)
  Tuple projection_records_;  // temp projection record(added by flerovium)
  Schema schema_;             // scan后生成的记录的字段

  std::unordered_set<std::string> seen_;

  bool is_unique_;  // 是否select unique的结果集

 public:
  ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols,
                     bool is_unique = false);

  void beginTuple() override;

  void nextTuple() override;

  std::unique_ptr<Tuple> Next() override;
  // std::unique_ptr<RmRecord> Next() override { return std::make_unique<RmRecord>(projection_records_); }

  RID &rid() override { return _abstract_rid; }

  const Schema &schema() const override { return schema_; };
  // const std::vector<ColMeta> &cols() const override { return cols_; };

  size_t tupleLen() const override { return len_; };

  bool IsEnd() const override { return prev_->IsEnd(); };

  // ColMeta get_col_offset(const TabCol &target) override {
  //   for (auto &col : cols_) {
  //     if (target.col_name == col.name && target.tab_name == col.tab_name) {
  //       return col;
  //     }
  //   }
  //   throw ColumnNotFoundError(target.col_name);
  // };

 private:
  // RmRecord projectRecord();
  Tuple projectRecord();

  std::string generate_new_name(const TabCol &col);
};
}  // namespace easydb