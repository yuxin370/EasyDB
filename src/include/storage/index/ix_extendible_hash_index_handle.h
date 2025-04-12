/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_defs.h
 *
 * Identification: src/include/storage/index/ix_defs.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "buffer/buffer_pool_manager.h"
#include "common/rid.h"
#include "storage/disk/disk_manager.h"
#include "storage/index/ix_defs.h"
#include "storage/page/page.h"

namespace easydb {

/* 管理中的每个节点 */
class IxBucketHandle {
  friend class IxExtendibleHashIndexHandle;
  friend class IxScan;

 private:
  const ExtendibleHashIxFileHdr *file_hdr;  // 节点所在文件的头部信息
  Page *page;                               // 存储桶的页面
  IxExtendibleHashPageHdr *page_hdr;  // page->data的第一部分，指针指向首地址，长度为sizeof(IxExtendibleHashPageHdr)
  char *keys;  // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
  RID *rids;   // page->data的第三部分，指针指向首地址

 public:
  IxBucketHandle() = default;

  IxBucketHandle(const ExtendibleHashIxFileHdr *file_hdr_, Page *page_) : file_hdr(file_hdr_), page(page_) {
    page_hdr = reinterpret_cast<IxExtendibleHashPageHdr *>(page->GetData());
    keys = page->GetData() + sizeof(IxExtendibleHashPageHdr);
    rids = reinterpret_cast<RID *>(keys + file_hdr->keys_size_);
  }

  IxBucketHandle(const ExtendibleHashIxFileHdr *file_hdr_, Page *page_, int local_depth, int size, bool is_valid)
      : file_hdr(file_hdr_), page(page_) {
    page_hdr = reinterpret_cast<IxExtendibleHashPageHdr *>(page->GetData());
    page_hdr->local_depth = local_depth;
    page_hdr->size = size;
    page_hdr->is_valid = is_valid;
    keys = page->GetData() + sizeof(IxExtendibleHashPageHdr);
    rids = reinterpret_cast<RID *>(keys + file_hdr->keys_size_);
  }

  int get_size() { return page_hdr->size; }

  void set_size(int size) { page_hdr->size = size; }

  int key_at(int i) { return *(int *)get_key(i); }

  page_id_t value_at(int i) { return get_rid(i)->GetPageId(); }

  page_id_t get_page_no() { return page->GetPageId().page_no; }

  PageId get_page_id() { return page->GetPageId(); }

  char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

  RID *get_rid(int rid_idx) const { return &rids[rid_idx]; }

  void set_key(int key_idx, const char *key) {
    memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_);
  }

  void set_rid(int rid_idx, const RID &rid) { rids[rid_idx] = rid; }

  // bucket操作
  bool IsFull() { return page_hdr->size == page_hdr->key_nums; }

  int GetLocalDepth() { return page_hdr->local_depth; }

  void IncrementLocalDepth() { page_hdr->local_depth++; }

  // int operator[](int index);

  int GetNumOfKeys() { return page_hdr->key_nums; }

  void Clear() { page_hdr->key_nums = 0; }

  int Insert(const char *key, const RID &value);

  int Insert(int pos, const char *key, const RID &value);

  int Remove(const char *key);

  bool Find(const char *key);

  int Find(const char *key, std::vector<RID> *result);

  // bool TestRuntimeError(const char *key);  // 没懂干啥的。

  void Reorganize(int pos);

  void DoubleDirectory(int new_size);

  void Update(int old_idx, int new_idx);
};

/* extendible hash  */
class IxExtendibleHashIndexHandle {
  friend class IxScan;
  friend class IxManager;

 private:
  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  int fd_;                             // 存储可扩展hash的文件
  ExtendibleHashIxFileHdr *file_hdr_;  // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
  std::mutex root_latch_;
  int global_depth;     // Record the global depth of the hash table
  int size_per_bucket;  // Size of each bucket

 public:
  IxExtendibleHashIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

  ~IxExtendibleHashIndexHandle();

  // for search
  bool GetValue(const char *key, std::vector<RID> *result);

  // for insert
  page_id_t InsertEntry(const char *key, const RID &value);

  // for delete
  bool DeleteEntry(const char *key);

  bool Erase();

  int getGlobalDepth() { return global_depth; }

  // hash function
  int HashFunction(const char *key, int n);

 private:
  void ReleaseBucketHandle(IxBucketHandle &bucket);

  // for index test
  RID GetRid(const Iid &iid) const;

  // split the bucket
  void SplitBucket(int originalBucket);

  // double the num of buckets
  void DoubleDirectory();

  // update pointers in the bucket
  void UpdatePointers(int index, int new_local_depth);

  // create a new bucket
  IxBucketHandle *CreateBucket(int index, const char *key, int new_local_depth);

  IxBucketHandle *FetchBucket(int page_no) const;

  IxBucketHandle *FindBucketPage(const char *key);

  IxBucketHandle *FindBucketPage(int index);
};

}  // namespace easydb
