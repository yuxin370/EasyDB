/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#include "execution/execution_manager.h"
#include "catalog/schema.h"
#include "common/errors.h"
#include "execution/executor_aggregation.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_merge_join.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_sort.h"
#include "parser/ast.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "type/value.h"

namespace easydb {

const char *help_info =
    "Supported SQL syntax:\n"
    "  command ;\n"
    "command:\n"
    "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
    "  DROP TABLE table_name\n"
    "  CREATE INDEX table_name (column_name)\n"
    "  DROP INDEX table_name (column_name)\n"
    "  INSERT INTO table_name VALUES (value [, value ...])\n"
    "  DELETE FROM table_name [WHERE where_clause]\n"
    "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
    "  SELECT selector FROM table_name [WHERE where_clause]\n"
    "type:\n"
    "  {INT | FLOAT | CHAR(n)}\n"
    "where_clause:\n"
    "  condition [AND condition ...]\n"
    "condition:\n"
    "  column op {column | value}\n"
    "column:\n"
    "  [table_name.]column_name\n"
    "op:\n"
    "  {= | <> | < | > | <= | >=}\n"
    "selector:\n"
    "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context) {
  if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
    switch (x->tag) {
      case T_CreateTable: {
        sm_manager_->CreateTable(x->tab_name_, x->cols_, context);
        break;
      }
      case T_DropTable: {
        sm_manager_->DropTable(x->tab_name_, context);
        break;
      }
      case T_CreateIndex: {
        sm_manager_->CreateIndex(x->tab_name_, x->tab_col_names_, context);
        break;
      }
      case T_DropIndex: {
        sm_manager_->DropIndex(x->tab_name_, x->tab_col_names_, context);
        break;
      }
      default:
        throw InternalError("Unexpected field type");
        break;
    }
  }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
  if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
    switch (x->tag) {
      case T_Help: {
        memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
        *(context->offset_) = strlen(help_info);
        break;
      }
      case T_ShowTable: {
        sm_manager_->ShowTables(context);
        break;
      }
      case T_ShowIndex: {
        sm_manager_->ShowIndex(x->tab_name_, context);
        break;
      }
      case T_DescTable: {
        sm_manager_->DescTable(x->tab_name_, context);
        break;
      }
      case T_Transaction_begin: {
        // 显示开启一个事务
        context->txn_->SetTxnMode(true);
        break;
      }
      case T_Transaction_commit: {
        context->txn_ = txn_mgr_->GetTransaction(*txn_id);
        txn_mgr_->Commit(context->txn_, context->log_mgr_);
        break;
      }
      case T_Transaction_rollback: {
        context->txn_ = txn_mgr_->GetTransaction(*txn_id);
        txn_mgr_->Abort(context->txn_, context->log_mgr_);
        break;
      }
      case T_Transaction_abort: {
        context->txn_ = txn_mgr_->GetTransaction(*txn_id);
        txn_mgr_->Abort(context->txn_, context->log_mgr_);
        break;
      }
      case T_CreateStaticCheckpoint: {
        context->txn_ = txn_mgr_->GetTransaction(*txn_id);
        txn_mgr_->CreateStaticCheckpoint(context->txn_, context->log_mgr_);
        break;
      }
      default:
        throw InternalError("Unexpected field type");
        break;
    }

  } else if (auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
    switch (x->set_knob_type_) {
      case ast::SetKnobType::EnableNestLoop: {
        planner_->set_enable_nestedloop_join(x->bool_value_);
        break;
      }
      case ast::SetKnobType::EnableSortMerge: {
        planner_->set_enable_sortmerge_join(x->bool_value_);
        break;
      }
      case ast::SetKnobType::EnableHashJoin: {
        planner_->setEnableHashJoin(x->bool_value_);
        break;
      }
      case ast::SetKnobType::EnableOptimizer: {
        planner_->SetEnableOptimizer(x->bool_value_);
      }
      case ast::SetKnobType::EnableOutput: {
        sm_manager_->SetEnableOutput(x->bool_value_);
        break;
      }
      default: {
        throw EASYDBError("Not implemented!\n");
        break;
      }
    }
  } else if (auto x = std::dynamic_pointer_cast<LoadDataPlan>(plan)) {
    // assert(x->tag == T_LoadData);
    sm_manager_->AsyncLoadData(x->file_name_, x->tab_name_, context);
    // sm_manager_->LoadData(x->file_name_, x->tab_name_, context);
  }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                            Context *context) {
  std::vector<std::string> captions;
  captions.reserve(sel_cols.size());
  for (auto &sel_col : sel_cols) {
    if (!sel_col.new_col_name.empty() || sel_col.aggregation_type != NO_AGG) {
      captions.push_back(sel_col.new_col_name);
    } else {
      captions.push_back(sel_col.col_name);
    }
  }

  // Print header into buffer
  RecordPrinter rec_printer(sel_cols.size());
  rec_printer.print_separator(context);
  rec_printer.print_record(captions, context);
  rec_printer.print_separator(context);
  // print header into file
  std::fstream outfile;
  outfile.open("output.txt", std::ios::out | std::ios::app);
  bool enable_output = sm_manager_->IsEnableOutput();
  bool print_caption = false;

  // Print records
  size_t num_rec = 0;
  // 执行query_plan
  for (executorTreeRoot->beginTuple(); !executorTreeRoot->IsEnd(); executorTreeRoot->nextTuple()) {
    auto tuple = executorTreeRoot->Next();
    if (num_rec == 0 && executorTreeRoot->IsEnd()) {
      outfile << "empty set\n\0";
    }
    std::vector<std::string> columns;
    std::string col_str;
    auto schema = &executorTreeRoot->schema();
    int column_count = schema->GetColumnCount();
    for (int column_itr = 0; column_itr < column_count; column_itr++) {
      if (tuple->IsNull(schema, column_itr)) {
        col_str = "NULL";
      } else {
        Value val = (tuple->GetValue(schema, column_itr));
        col_str = val.ToString();
      }
      columns.emplace_back(col_str);
    }

    if (!print_caption && enable_output) {
      outfile << "|";
      for (int i = 0; i < captions.size(); ++i) {
        outfile << " " << captions[i] << " |";
      }
      outfile << "\n";
      print_caption = true;
    }
    // print record into buffer
    rec_printer.print_record(columns, context);
    // print record into file
    if (enable_output) {
      outfile << "|";
      for (int i = 0; i < columns.size(); ++i) {
        outfile << " " << columns[i] << " |";
      }
      outfile << "\n";
    }
    num_rec++;
  }
  if (!print_caption && enable_output) {
    outfile << "|";
    for (int i = 0; i < captions.size(); ++i) {
      outfile << " " << captions[i] << " |";
    }
    outfile << "\n";
  }
  outfile.close();
  // Print footer into buffer
  rec_printer.print_separator(context);
  // Print record count into buffer
  RecordPrinter::print_record_count(num_rec, context);
}

/*
execute select stmt in subquery

*/
std::vector<Value> subquery_select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot, TabCol sel_col) {
  std::vector<Value> outputs;
  // 执行query_plan
  for (executorTreeRoot->beginTuple(); !executorTreeRoot->IsEnd(); executorTreeRoot->nextTuple()) {
    auto Tuple = executorTreeRoot->Next();
    Value output;
    std::vector<std::string> columns;
    if (executorTreeRoot->cols().size() != 1) {
      throw InternalError("subquery executorTreeRoot->cols().size() should be 1\n");
    }

    // auto col = executorTreeRoot->cols()[0];
    auto col = executorTreeRoot->schema().GetColumn(0);
    std::string col_str;
    // char *rec_buf = Tuple->data + col.GetOffset();
    char *tp = new char[Tuple->GetLength()];
    memcpy(tp, Tuple->GetData(), Tuple->GetLength());
    char *rec_buf = tp + col.GetOffset();

    if (col.GetType() == TYPE_INT) {
      output = Value().DeserializeFrom(rec_buf, TypeId::TYPE_INT);
    } else if (col.GetType() == TYPE_FLOAT) {
      output = Value().DeserializeFrom(rec_buf, TypeId::TYPE_FLOAT);
    } else if (col.GetType() == TYPE_VARCHAR || col.GetType() == TYPE_CHAR) {
      uint32_t size = *reinterpret_cast<const uint32_t *>(col.GetStorageSize());
      char *str_tp = new char[Tuple->GetLength() + sizeof(uint32_t)];
      memcpy(str_tp, (char *)size, sizeof(uint32_t));
      memcpy(str_tp + sizeof(uint32_t), rec_buf, size);
      output = Value().DeserializeFrom(str_tp, TypeId::TYPE_VARCHAR);
      delete[] str_tp;
    }
    delete[] tp;
    outputs.push_back(output);
  }
  return outputs;
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) { exec->Next(); }

}  // namespace easydb
