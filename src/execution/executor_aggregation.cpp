/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "execution/executor_aggregation.h"

/**
 * e.g.
 *
 * select SUM(score) as sum_score from grade where id = 1;
 *
 * select course ,COUNT(*) as row_num , COUNT(id) as student_num
 * from grade
 * group by course;
 *
 * select id,MAX(score) as max_score,MIN(score) as min_score
 * from grade
 * group by id
 * having COUNT(*) > 1 and MIN(score) > 88;
 *
 * TYPE: COUNT MIN MAX SUM
 *
 * enum AggregationType {
 *  MAX_AGG, MIN_AGG, COUNT_AGG, SUM_AGG, NO_AGG
 *  };
 */

namespace easydb {

AggregationExecutor::AggregationExecutor(SmManager *sm_manager, std::unique_ptr<AbstractExecutor> prev,
                                         std::vector<TabCol> sel_col_) {
  //  std::vector<TabCol> group_cols, std::vector<Condition> having_conds) {
  sm_manager_ = sm_manager;
  prev_ = std::move(prev);
  prev_schema_ = prev_->schema();
  tab_name_ = prev_->getTabName();
  int offset = 0;
  std::vector<Column> new_colus_;
  for (auto &sel_col : sel_col_) {
    Column colu_tp;
    if (sel_col.col_name == "*" && sel_col.aggregation_type == AggregationType::COUNT_AGG) {
      colu_tp.SetTabName("");
      colu_tp.SetName("*");
      colu_tp.SetType(TYPE_INT);
      colu_tp.SetStorageSize(sizeof(int));
      colu_tp.SetOffset(offset);
      colu_tp.SetAggregationType(AggregationType::COUNT_AGG);
      sel_colus_.push_back(colu_tp);
      colu_tp.SetName(generate_new_name(sel_col));
      new_colus_.push_back(colu_tp);
      offset += sizeof(int);
      continue;
    }

    colu_tp = prev_schema_.GetColumn(sel_col.col_name);

    colu_tp.SetAggregationType(sel_col.aggregation_type);
    sel_colus_.push_back(colu_tp);
    if (sel_col.aggregation_type != AggregationType::NO_AGG) {
      if (sel_col.aggregation_type == AggregationType::COUNT_AGG) {
        colu_tp.SetType(ColType::TYPE_INT);
        colu_tp.SetStorageSize(sizeof(int));
      } else if (sel_col.aggregation_type == AggregationType::SUM_AGG &&
                 (colu_tp.GetType() == ColType::TYPE_VARCHAR || colu_tp.GetType() == ColType::TYPE_CHAR)) {
        throw InternalError("string type do not supports sum aggreagation.");
      }
      colu_tp.SetName(generate_new_name(sel_col));
    }
    colu_tp.SetOffset(offset);
    offset += colu_tp.GetStorageSize();
    new_colus_.push_back(colu_tp);
  }
  schema_ = Schema(new_colus_);

  len_ = offset;
  isend_ = false;
}

void AggregationExecutor::beginTuple() {
  // do nothing

}

void AggregationExecutor::nextTuple() {
  isend_ = true;
}

std::unique_ptr<Tuple> AggregationExecutor::Next() {
  std::vector<Value> value_vec;
  for (auto &sel_col : sel_colus_) {
    value_vec.push_back(aggregation_to_value(sel_col));
  }
  return std::make_unique<Tuple>(value_vec, &schema_);
}

Value AggregationExecutor::aggregation_to_value(Column target_colu) {
  AggregationType type = target_colu.GetAggregationType();
  std::string col_name = target_colu.GetName();
  int length = target_colu.GetStorageSize();
  Value val;
  switch (type) {
    // case AggregationType::NO_AGG:
    case AggregationType::SUM_AGG:
      if(target_colu.GetType() == TypeId::TYPE_INT){
        return Value(target_colu.GetType(), int(sm_manager_->GetTableAttrSum(tab_name_, col_name)));
      }else{
        return Value(target_colu.GetType(), sm_manager_->GetTableAttrSum(tab_name_, col_name));
      }
    case AggregationType::COUNT_AGG:
      return Value(TypeId::TYPE_INT, sm_manager_->GetTableCount(tab_name_));
    case AggregationType::MAX_AGG:
      if(target_colu.GetType() == TypeId::TYPE_INT){
        return Value(target_colu.GetType(), int(sm_manager_->GetTableAttrMax(tab_name_, col_name)));
      }else{
        return Value(target_colu.GetType(), sm_manager_->GetTableAttrMax(tab_name_, col_name));
      }
    case AggregationType::MIN_AGG:
      if(target_colu.GetType() == TypeId::TYPE_INT){
        return Value(target_colu.GetType(), int(sm_manager_->GetTableAttrMin(tab_name_, col_name)));
      }else{
        return Value(target_colu.GetType(), sm_manager_->GetTableAttrMin(tab_name_, col_name));
      }
    default:
      throw InternalError("unsupported aggregation operator.");
  }
}

std::string AggregationExecutor::generate_new_name(TabCol col) {
  if (col.new_col_name != "") {
    return col.new_col_name;
  }
  switch (col.aggregation_type) {
    case MAX_AGG:
      return "MAX(" + col.col_name + ")";
    case MIN_AGG:
      return "MIN(" + col.col_name + ")";
    case COUNT_AGG:
      return "COUNT(" + col.col_name + ")";
    case SUM_AGG:
      return "SUM(" + col.col_name + ")";
    default:
      throw InternalError("unsupported aggregation type.");
  }
}

}  // namespace easydb
