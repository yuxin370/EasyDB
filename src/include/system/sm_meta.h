/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/errors.h"
#include "common/exception.h"
#include "sm_defs.h"
#include "type/type_id.h"

namespace easydb {

/* 字段元数据 */
struct ColMeta {
  std::string tab_name;  // 字段所属表名称
  std::string name;      // 字段名称
  ColType type;          // 字段类型
  int len;               // 字段长度
  int offset;            // 字段位于记录中的偏移量
  bool index;            /** unused */
  AggregationType agg_type = NO_AGG;

  ColMeta() {}

  ColMeta(std::string tab_name_, std::string name_, ColType type_, int len_, int offset_, bool index_)
      : tab_name(tab_name_), name(name_), type(type_), len(len_), offset(offset_), index(index_) {
    agg_type = NO_AGG;
  }

  ColMeta(Column &column) {
    name = column.GetName();
    offset = column.GetOffset();
    type = column.GetType();
    len = column.GetStorageSize();
    index = false;
  }

  friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
    // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
    return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
              << col.index;
  }

  friend std::istream &operator>>(std::istream &is, ColMeta &col) {
    return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index;
  }
};

/* 索引元数据 */
struct IndexMeta {
  std::string tab_name;           // 索引所属表名称
  int col_tot_len;                // 索引字段长度总和
  int col_num;                    // 索引字段数量
  std::vector<ColMeta> cols;      // 索引包含的字段
  std::vector<uint32_t> col_ids;  // 索引字段在表schema中的位置
  // Schema schema;

  // IndexMeta() {}

  friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index) {
    os << index.tab_name << " " << index.col_tot_len << " " << index.col_num;
    for (auto &col : index.cols) {
      os << "\n" << col;
    }
    for (auto &col_index : index.col_ids) {
      os << "\n" << col_index;
    }
    return os;
  }

  friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
    is >> index.tab_name >> index.col_tot_len >> index.col_num;
    for (int i = 0; i < index.col_num; ++i) {
      ColMeta col;
      is >> col;
      index.cols.push_back(col);
    }
    for (int i = 0; i < index.col_num; ++i) {
      uint32_t col_index;
      is >> col_index;
      index.col_ids.push_back(col_index);
    }
    return is;
  }
};

/* 表元数据 */
struct TabMeta {
  std::string name;                // 表名称
  std::vector<ColMeta> cols;       // 表包含的字段
  std::vector<IndexMeta> indexes;  // 表上建立的索引
  Schema schema;

  TabMeta() {}

  TabMeta(std::string name_, std::vector<ColMeta> cols_, std::vector<IndexMeta> indexes_, Schema schema_)
      : name(name_), cols(cols_), indexes(indexes_), schema(schema_) {}

  TabMeta(const TabMeta &other) {
    name = other.name;
    for (auto col : other.cols) cols.push_back(col);
  }

  /* 判断当前表中是否存在名为col_name的字段 */
  bool is_col(const std::string &col_name) const {
    auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
    return pos != cols.end();
  }

  /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
  bool is_index(const std::vector<std::string> &col_names) const {
    for (auto &index : indexes) {
      if (index.col_num == col_names.size()) {
        size_t i = 0;
        for (; i < index.col_num; ++i) {
          if (index.cols[i].name.compare(col_names[i]) != 0) break;
        }
        if (i == index.col_num) return true;
      }
    }

    return false;
  }

  /* 根据字段名称集合获取索引元数据 */
  std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string> &col_names) {
    for (auto index = indexes.begin(); index != indexes.end(); ++index) {
      if ((*index).col_num != col_names.size()) continue;
      auto &index_cols = (*index).cols;
      size_t i = 0;
      for (; i < col_names.size(); ++i) {
        if (index_cols[i].name.compare(col_names[i]) != 0) break;
      }
      if (i == col_names.size()) return index;
    }
    throw IndexNotFoundError(name, col_names);
  }

  /* 根据字段名称获取字段元数据 */
  std::vector<ColMeta>::iterator get_col(const std::string &col_name) {
    auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
    if (pos == cols.end()) {
      throw ColumnNotFoundError(col_name);
    }
    return pos;
  }

  /* 根据字段名称获取字段在schema中的位置(0..字段数量-1) */
  uint32_t GetColId(const std::string &col_name) { return get_col(col_name) - cols.begin(); }

  friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
    os << tab.name << '\n' << tab.cols.size() << '\n';
    for (auto &col : tab.cols) {
      os << col << '\n';  // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
    }
    os << tab.indexes.size() << "\n";
    for (auto &index : tab.indexes) {
      os << index << "\n";
    }
    os << tab.schema.ToString(false) << '\n';
    return os;
  }

  friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
    size_t n;
    is >> tab.name >> n;
    for (size_t i = 0; i < n; i++) {
      ColMeta col;
      is >> col;
      tab.cols.push_back(col);
    }
    is >> n;
    for (size_t i = 0; i < n; ++i) {
      IndexMeta index;
      is >> index;
      tab.indexes.push_back(index);
    }
    // TODO
    // is >> tab.schema;
    return is;
  }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和 >>
/* 数据库元数据 */
class DbMeta {
  friend class SmManager;

 private:
  std::string name_;                     // 数据库名称
  std::map<std::string, TabMeta> tabs_;  // 数据库中包含的表

 public:
  // DbMeta(std::string name) : name_(name) {}

  /* 判断数据库中是否存在指定名称的表 */
  bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

  void SetTabMeta(const std::string &tab_name, const TabMeta &meta) { tabs_[tab_name] = meta; }

  /* 获取指定名称表的元数据
   * @note 返回值为引用，调用者需要保留引用
   */
  TabMeta &get_table(const std::string &tab_name) {
    auto pos = tabs_.find(tab_name);
    if (pos == tabs_.end()) {
      throw TableNotFoundError(tab_name);
    }

    return pos->second;
  }

  // 重载操作符 <<
  friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta) {
    os << db_meta.name_ << '\n' << db_meta.tabs_.size() << '\n';
    for (auto &entry : db_meta.tabs_) {
      os << entry.second << '\n';
    }
    return os;
  }

  friend std::istream &operator>>(std::istream &is, DbMeta &db_meta) {
    size_t n;
    is >> db_meta.name_ >> n;
    for (size_t i = 0; i < n; i++) {
      TabMeta tab;
      is >> tab;
      db_meta.tabs_[tab.name] = tab;
    }
    return is;
  }
};

}  // namespace easydb
