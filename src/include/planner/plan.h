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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "common/condition.h"
#include "parser/ast.h"

#include "parser/parser.h"

#include "common/common.h"
#include "system/sm_manager.h"

namespace easydb {
typedef enum PlanTag {
  T_Invalid = 1,
  T_Help,
  T_ShowTable,
  T_ShowIndex,
  T_DescTable,
  T_CreateTable,
  T_DropTable,
  T_CreateIndex,
  T_DropIndex,
  T_CreateStaticCheckpoint,
  T_LoadData,
  T_SetKnob,
  T_Insert,
  T_Update,
  T_Delete,
  T_select,
  T_Transaction_begin,
  T_Transaction_commit,
  T_Transaction_abort,
  T_Transaction_rollback,
  T_SeqScan,
  T_IndexScan,
  T_NestLoop,
  T_SortMerge,   // sort merge join
  T_IndexMerge,  // merge join using index
  T_HashJoin,
  T_Sort,
  T_Projection,
  T_Aggregation,
  T_Empty  // ADDED: 表示空结果集的执行计划
} PlanTag;

// 查询执行计划
class Plan {
 public:
  PlanTag tag;
  virtual ~Plan() = default;
  std::vector<Condition> get_conds() { throw std::runtime_error("not supported!"); }
};

// ADDED: 定义EmptyPlan类
class EmptyPlan : public Plan {
 public:
  EmptyPlan() { tag = T_Empty; }
  ~EmptyPlan() {}
};

class ScanPlan : public Plan {
 public:
  ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
           std::vector<std::string> index_col_names) {
    Plan::tag = tag;
    tab_name_ = std::move(tab_name);
    conds_ = std::move(conds);
    TabMeta &tab = sm_manager->db_.get_table(tab_name_);
    cols_ = tab.cols;
    len_ = cols_.back().offset + cols_.back().len;
    fed_conds_ = conds_;
    index_col_names_ = index_col_names;
  }
  ~ScanPlan() {}

  // bool is_index_scan(){
  //   return tag == T_IndexScan;
  // }

  std::vector<Condition> get_conds() { return fed_conds_; }

  // 以下变量同ScanExecutor中的变量
  std::string tab_name_;
  std::vector<ColMeta> cols_;
  std::vector<Condition> conds_;
  size_t len_;
  std::vector<Condition> fed_conds_;
  std::vector<std::string> index_col_names_;
};

class JoinPlan : public Plan {
 public:
  JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds) {
    Plan::tag = tag;
    left_ = std::move(left);
    right_ = std::move(right);
    conds_ = std::move(conds);
    type = INNER_JOIN;
  }
  ~JoinPlan() {}
  // 左节点
  std::shared_ptr<Plan> left_;
  // 右节点
  std::shared_ptr<Plan> right_;
  // 连接条件
  std::vector<Condition> conds_;
  // future TODO: 后续可以支持的连接类型
  JoinType type;
};

class ProjectionPlan : public Plan {
 public:
  ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols, bool is_unique = false) {
    Plan::tag = tag;
    subplan_ = std::move(subplan);
    sel_cols_ = std::move(sel_cols);
    is_unique_ = is_unique;
  }
  ~ProjectionPlan() {}
  void SetUnique(bool is_unique) { is_unique_ = is_unique; }
  std::shared_ptr<Plan> subplan_;
  std::vector<TabCol> sel_cols_;
  bool is_unique_;
};

class SortPlan : public Plan {
 public:
  SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, TabCol sel_col, bool is_desc) {
    Plan::tag = tag;
    subplan_ = std::move(subplan);
    sel_col_ = sel_col;
    is_desc_ = is_desc;
  }
  ~SortPlan() {}
  std::shared_ptr<Plan> subplan_;
  TabCol sel_col_;
  bool is_desc_;
};

class AggregationPlan : public Plan {
 public:
  AggregationPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols,
                  std::vector<TabCol> group_cols, std::vector<Condition> having_conds) {
    Plan::tag = tag;
    subplan_ = std::move(subplan);
    sel_cols_ = std::move(sel_cols);
    group_cols_ = std::move(group_cols);
    having_conds_ = std::move(having_conds);
  }
  ~AggregationPlan() {}
  std::shared_ptr<Plan> subplan_;
  std::vector<TabCol> sel_cols_;
  std::vector<TabCol> group_cols_;
  std::vector<Condition> having_conds_;
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan {
 public:
  DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::string tab_name, std::vector<Value> values,
          std::vector<Condition> conds, std::vector<SetClause> set_clauses, bool unique = false) {
    Plan::tag = tag;
    subplan_ = std::move(subplan);
    tab_name_ = std::move(tab_name);
    values_ = std::move(values);
    conds_ = std::move(conds);
    set_clauses_ = std::move(set_clauses);
    unique_ = unique;
  }
  DMLPlan(std::shared_ptr<void> &ptr) {
    auto derived_ptr = std::static_pointer_cast<DMLPlan>(ptr);
    if (!derived_ptr) {
      throw std::bad_cast();
    }

    // 使用 derived_ptr 进行成员变量初始化
    Plan::tag = derived_ptr->tag;
    subplan_ = derived_ptr->subplan_;
    tab_name_ = derived_ptr->tab_name_;
    values_ = derived_ptr->values_;
    conds_ = derived_ptr->conds_;
    set_clauses_ = derived_ptr->set_clauses_;
    unique_ = derived_ptr->unique_;
  }
  ~DMLPlan() {}
  std::shared_ptr<Plan> subplan_;
  std::string tab_name_;
  std::vector<Value> values_;
  std::vector<Condition> conds_;
  std::vector<SetClause> set_clauses_;
  bool unique_;  // unique select
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan {
 public:
  DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols) {
    Plan::tag = tag;
    tab_name_ = std::move(tab_name);
    cols_ = std::move(cols);
    tab_col_names_ = std::move(col_names);
  }
  ~DDLPlan() {}
  std::string tab_name_;
  std::vector<std::string> tab_col_names_;
  std::vector<ColDef> cols_;
};

// load data语句对应的plan
class LoadDataPlan : public Plan {
 public:
  LoadDataPlan(PlanTag tag, std::string file_name, std::string tab_name) {
    Plan::tag = tag;
    file_name_ = std::move(file_name);
    tab_name_ = std::move(tab_name);
  }
  ~LoadDataPlan() {}
  std::string file_name_;
  std::string tab_name_;
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan {
 public:
  OtherPlan(PlanTag tag, std::string tab_name) {
    Plan::tag = tag;
    tab_name_ = std::move(tab_name);
  }
  ~OtherPlan() {}
  std::string tab_name_;
};

// Set Knob Plan
class SetKnobPlan : public Plan {
 public:
  SetKnobPlan(ast::SetKnobType knob_type, bool bool_value) {
    Plan::tag = T_SetKnob;
    set_knob_type_ = knob_type;
    bool_value_ = bool_value;
  }
  ast::SetKnobType set_knob_type_;
  bool bool_value_;
};

class plannerInfo {
 public:
  std::shared_ptr<ast::SelectStmt> parse;
  std::vector<Condition> where_conds;
  std::vector<TabCol> sel_cols;
  std::shared_ptr<Plan> plan;
  std::vector<std::shared_ptr<Plan>> table_scan_executors;
  std::vector<SetClause> set_clauses;
  plannerInfo(std::shared_ptr<ast::SelectStmt> parse_) : parse(std::move(parse_)) {}
};
};  // namespace easydb