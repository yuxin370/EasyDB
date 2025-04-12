/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
#include "parser/ast.h"
#undef NDEBUG

#include <cassert>

#include "parser/ast_dot_printer.h"
#include "parser/parser.h"

int main() {
  std::vector<std::string> sqls = {
      // "CREATE TABLE SUPPLIER ( S_SUPPKEY INTEGER NOT NULL, S_NAME CHAR(25) NOT NULL,\
      //    S_ADDRESS VARCHAR(40) NOT NULL, S_NATIONKEY INTEGER NOT NULL, S_PHONE CHAR(15) NOT NULL,\
      //     S_ACCTBAL FLOAT NOT NULL, S_COMMENT VARCHAR(101) NOT NULL);",
      // "select name from student where id in (1,2,3);",
      // "select name,count(*) as count,sum(val) as sum_val, max(val) as max_val,min(val) as min_val \
      //    from aggregate group by name;",
      // "select id,MAX(score) as max_score from grade group by id having COUNT(*) > 3;",
      // "create static_checkpoint;",
      // "load ../../src/test/performance_test/table_data/warehouse.csv into warehouse;",
      // "set output_file off",
      "set enable_optimizer = true;",
      "create table history (h_c_id int, h_date datetime);",
      // "exit;",
      // "help;",
      // "",
      // "update student set score = score + 1 where score > 5.5;",
      // "select * from student;",
      // "select unique * from student;",
      // "SELECT * FROM supplier, customer, nation where S_SUPPKEY < 10 AND C_CUSTKEY < 10 AND S_NATIONKEY = N_NATIONKEY
      // "
      // "AND C_NATIONKEY = N_NATIONKEY; "
  };
  for (auto &sql : sqls) {
    std::cout << sql << std::endl;
    YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
    assert(yyparse() == 0);
    if (ast::parse_tree != nullptr) {
      ast::TreeDotPrinter tdp("ast_out.dot");
      // ast::TreePrinter::print(ast::parse_tree);
      tdp.print(ast::parse_tree);
      yy_delete_buffer(buf);
      std::cout << std::endl;
    } else {
      yy_delete_buffer(buf);
      std::cout << "exit/EOF" << std::endl;
    }
  }
  ast::parse_tree.reset();
  return 0;
}
