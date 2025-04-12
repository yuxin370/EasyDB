%{
#include "parser/ast.h"
#include "yacc.tab.h"
#include <iostream>
#include <memory>

int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);

void yyerror(YYLTYPE *locp, const char* s) {
    std::cerr << "Parser Error at line " << locp->first_line << " column " << locp->first_column << ": " << s << std::endl;
}

using namespace ast;
%}

// request a pure (reentrant) parser
%define api.pure full
// enable location in error handler
%locations
// enable verbose syntax error message
%define parse.error verbose

// keywords
%token SHOW TABLES CREATE TABLE DROP DESC INSERT INTO VALUES DELETE FROM ASC ORDER BY AS COUNT MAX MIN SUM GROUP HAVING IN
WHERE UPDATE SET SELECT INT CHAR VARCHAR FLOAT DATETIME NOT_NULL INDEX AND JOIN EXIT HELP TXN_BEGIN TXN_COMMIT TXN_ABORT TXN_ROLLBACK ORDER_BY 
UNIQUE ENABLE_NESTLOOP ENABLE_SORTMERGE ENABLE_HASHJOIN ENABLE_OPTIMIZER
STATIC_CHECKPOINT LOAD OUTPUT_FILE

// non-keywords
%token LEQ NEQ GEQ T_EOF

// type-specific tokens
%token <sv_str> IDENTIFIER VALUE_STRING PATH_STRING
%token <sv_int> VALUE_INT
%token <sv_float> VALUE_FLOAT
%token <sv_bool> VALUE_BOOL

// specify types for non-terminal symbol
%type <sv_node> stmt dbStmt ddl dml txnStmt setStmt setOutputStmt
%type <sv_field> field
%type <sv_fields> fieldList
%type <sv_type_len> type
%type <sv_comp_op> op
%type <sv_arith_op> arith_op
%type <sv_expr> expr
%type <sv_arith_expr> arithExpr
%type <sv_val> value
%type <sv_vals> valueList
%type <sv_str> tbName colName fileName
%type <sv_strs> tableList colNameList
%type <sv_col> col
%type <sv_cols> colList selector
%type <sv_set_clause> setClause
%type <sv_set_clauses> setClauses
%type <sv_cond> condition
%type <sv_conds> whereClause optWhereClause
%type <sv_groupby> group_by_clause_opt
%type <sv_having> having_clause_opt
%type <sv_orderby>  order_clause opt_order_clause
%type <sv_orderby_dir> opt_asc_desc
%type <sv_setKnobType> set_knob_type

%%
start:
        stmt ';'
    {
        parse_tree = $1;
        YYACCEPT;
    }
    |   setOutputStmt
    {
        parse_tree = $1;
        YYACCEPT;
    }
    |   HELP
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
    |   EXIT
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |   T_EOF
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    ;

stmt:
        dbStmt
    |   ddl
    |   dml
    |   txnStmt
    |   setStmt
    ;

txnStmt:
        TXN_BEGIN
    {
        $$ = std::make_shared<TxnBegin>();
    }
    |   TXN_COMMIT
    {
        $$ = std::make_shared<TxnCommit>();
    }
    |   TXN_ABORT
    {
        $$ = std::make_shared<TxnAbort>();
    }
    | TXN_ROLLBACK
    {
        $$ = std::make_shared<TxnRollback>();
    }
    ;

dbStmt:
        SHOW TABLES
    {
        $$ = std::make_shared<ShowTables>();
    }
    |   SHOW INDEX FROM tbName
    {
        $$ = std::make_shared<ShowIndex>($4);
    }
    ;

setStmt:
        SET set_knob_type '=' VALUE_BOOL
    {
        $$ = std::make_shared<SetStmt>($2, $4);
    }
    ;

setOutputStmt:
        SET set_knob_type VALUE_BOOL
    {
        $$ = std::make_shared<SetStmt>($2, $3);
    }
    ;

ddl:
        CREATE TABLE tbName '(' fieldList ')'
    {
        $$ = std::make_shared<CreateTable>($3, $5);
    }
    |   DROP TABLE tbName
    {
        $$ = std::make_shared<DropTable>($3);
    }
    |   DESC tbName
    {
        $$ = std::make_shared<DescTable>($2);
    }
    |   CREATE INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<CreateIndex>($3, $5);
    }
    |   DROP INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<DropIndex>($3, $5);
    }
    |   CREATE STATIC_CHECKPOINT
    {
        $$ = std::make_shared<CreateStaticCheckpoint>();
    }
    |   LOAD fileName INTO tbName
    {
        $$ = std::make_shared<LoadData>($2, $4);
    }
    ;

dml:
        INSERT INTO tbName VALUES '(' valueList ')'
    {
        $$ = std::make_shared<InsertStmt>($3, $6);
    }
    |   DELETE FROM tbName optWhereClause
    {
        $$ = std::make_shared<DeleteStmt>($3, $4);
    }
    |   UPDATE tbName SET setClauses optWhereClause
    {
        $$ = std::make_shared<UpdateStmt>($2, $4, $5);
    }
    |   SELECT UNIQUE selector FROM tableList optWhereClause group_by_clause_opt having_clause_opt opt_order_clause
    {
        $$ = std::make_shared<SelectStmt>($3, $5, $6, $7, $8, $9, true);
    }
    |   SELECT UNIQUE selector optWhereClause FROM tableList group_by_clause_opt having_clause_opt opt_order_clause
    {
        $$ = std::make_shared<SelectStmt>($3, $6, $4, $7, $8, $9, true);
    }
    |   SELECT selector FROM tableList optWhereClause group_by_clause_opt having_clause_opt opt_order_clause
    {
        $$ = std::make_shared<SelectStmt>($2, $4, $5, $6, $7, $8);
    }
    |   SELECT selector optWhereClause FROM tableList group_by_clause_opt having_clause_opt opt_order_clause
    {
        $$ = std::make_shared<SelectStmt>($2, $5, $3, $6, $7, $8);
    }
    ;

fieldList:
        field
    {
        $$ = std::vector<std::shared_ptr<Field>>{$1};
    }
    |   fieldList ',' field
    {
        $$.push_back($3);
    }
    ;

colNameList:
        colName
    {
        $$ = std::vector<std::string>{$1};
    }
    | colNameList ',' colName
    {
        $$.push_back($3);
    }
    ;

field:
        colName type
    {
        $$ = std::make_shared<ColDef>($1, $2);
    }
    |   colName type NOT_NULL
    {
        $$ = std::make_shared<ColDef>($1, $2, true);
    }
    ;

type:
        INT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
    |   CHAR '(' VALUE_INT ')'
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, $3);
    }
    |   VARCHAR '(' VALUE_INT ')'
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, $3);
    }
    |   FLOAT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
    |   DATETIME
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, 19);
    }
    ;

valueList:
        value
    {
        $$ = std::vector<std::shared_ptr<Value>>{$1};
    }
    |   valueList ',' value
    {
        $$.push_back($3);
    }
    ;

value:
        VALUE_INT
    {
        $$ = std::make_shared<IntLit>($1);
    }
    |   VALUE_FLOAT
    {
        $$ = std::make_shared<FloatLit>($1);
    }
    |   VALUE_STRING
    {
        $$ = std::make_shared<StringLit>($1);
    }
    |   VALUE_BOOL
    {
        $$ = std::make_shared<BoolLit>($1);
    }
    ;

condition:
        col op expr
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $3);
    }
    |   col op dml
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $3);
    }
    |   col op '(' dml ')'
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $4);
    }
    |   col op '(' valueList ')'
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $4);
    }
    ;

optWhereClause:
        /* epsilon */ { /* ignore*/ }
    |   WHERE whereClause
    {
        $$ = $2;
    }
    ;

whereClause:
        condition 
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    |   whereClause AND condition
    {
        $$.push_back($3);
    }
    ;

col:
        tbName '.' colName
    {
        $$ = std::make_shared<Col>($1, $3, "", NO_AGG);
    }
    |   colName
    {
        $$ = std::make_shared<Col>("", $1, "", NO_AGG);
    }
    |   COUNT '(' '*' ')' AS colName
    {
        $$ = std::make_shared<Col>("", "", $6, COUNT_AGG);
    }
    |   COUNT '(' colName ')' AS colName
    {
        $$ = std::make_shared<Col>("", $3, $6, COUNT_AGG);
    }
    |   COUNT '(' '*' ')' AS COUNT
    {
        $$ = std::make_shared<Col>("", "", "count", COUNT_AGG);
    }
    |   COUNT '(' colName ')' AS COUNT
    {
        $$ = std::make_shared<Col>("", $3, "count", COUNT_AGG);
    }
    |   MAX '(' colName ')' AS colName
    {
        $$ = std::make_shared<Col>("", $3, $6, MAX_AGG);
    }
    |   MIN '(' colName ')' AS colName
    {
        $$ = std::make_shared<Col>("", $3, $6, MIN_AGG);
    }
    |   SUM '(' colName ')' AS colName
    {
        $$ = std::make_shared<Col>("", $3, $6, SUM_AGG);
    }
    |   COUNT '(' '*' ')'
    {
        $$ = std::make_shared<Col>("", "", "", COUNT_AGG);
    }
    |   COUNT '(' colName ')' 
    {
        $$ = std::make_shared<Col>("", $3, "", COUNT_AGG);
    }
    |   MAX '(' colName ')' 
    {
        $$ = std::make_shared<Col>("", $3, "", MAX_AGG);
    }
    |   MIN '(' colName ')' 
    {
        $$ = std::make_shared<Col>("", $3, "", MIN_AGG);
    }
    |   SUM '(' colName ')'
    {
        $$ = std::make_shared<Col>("", $3, "", SUM_AGG);
    }
    ;

colList:
        col
    {
        $$ = std::vector<std::shared_ptr<Col>>{$1};
    }
    |   colList ',' col
    {
        $$.push_back($3);
    }
    ;

op:
        '='
    {
        $$ = SV_OP_EQ;
    }
    |   '<'
    {
        $$ = SV_OP_LT;
    }
    |   '>'
    {
        $$ = SV_OP_GT;
    }
    |   NEQ
    {
        $$ = SV_OP_NE;
    }
    |   LEQ
    {
        $$ = SV_OP_LE;
    }
    |   GEQ
    {
        $$ = SV_OP_GE;
    }
    |   IN
    {
        $$ = SV_OP_IN;
    }
    ;

arith_op:
        '+'
    {
        $$ = SV_OP_PLUS;
    }
    |   '-'
    {
        $$ = SV_OP_MINUS;
    }
    |   '*'
    {
        $$ = SV_OP_MUL;
    }
    |   '/'
    {
        $$ = SV_OP_DIV;
    }
    ;

expr:
        value
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    ;

setClauses:
        setClause
    {
        $$ = std::vector<std::shared_ptr<SetClause>>{$1};
    }
    |   setClauses ',' setClause
    {
        $$.push_back($3);
    }
    ;

arithExpr:
        colName arith_op value
    {
        $$ = std::make_shared<ArithExpr>($1, $2, $3);
    }
    ;

setClause:
        colName '=' arithExpr
    {
        $$ = std::make_shared<SetClause>($1, $3);
    }
    |   colName '=' value
    {
        $$ = std::make_shared<SetClause>($1, $3);
    }
    ;

selector:
        '*'
    {
        $$ = {};
    }
    |   colList
    ;

tableList:
        tbName
    {
        $$ = std::vector<std::string>{$1};
    }
    |   tableList ',' tbName
    {
        $$.push_back($3);
    }
    |   tableList JOIN tbName
    {
        $$.push_back($3);
    }
    ;
group_by_clause_opt:
    GROUP BY colList
    {
        $$ = std::make_shared<GroupBy>($3);
    }
    |   /* epsilon */ { /* ignore*/ }
    ;

having_clause_opt:
    HAVING whereClause
    {
        $$ = $2;
    }
    |   /* epsilon */ { /* ignore*/ }
    ;

opt_order_clause:
    ORDER BY order_clause      
    { 
        $$ = $3;
    }
    |   /* epsilon */ { /* ignore*/ }
    ;

order_clause:
      col  opt_asc_desc 
    { 
        $$ = std::make_shared<OrderBy>($1, $2);
    }
    ;   

opt_asc_desc:
    ASC          { $$ = OrderBy_ASC;     }
    |  DESC      { $$ = OrderBy_DESC;    }
    |       { $$ = OrderBy_DEFAULT; }
    ;    

set_knob_type:
    ENABLE_NESTLOOP { $$ = EnableNestLoop; }
    |   ENABLE_SORTMERGE { $$ = EnableSortMerge; }
    |   ENABLE_HASHJOIN { $$ = EnableHashJoin; }
    |   ENABLE_OPTIMIZER { $$ = EnableOptimizer; }
    |   OUTPUT_FILE { $$ = EnableOutput; }
    ;

tbName: IDENTIFIER;

colName: IDENTIFIER;

fileName: PATH_STRING;
%%
