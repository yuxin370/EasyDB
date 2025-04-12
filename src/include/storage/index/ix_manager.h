/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_manager.h
 *
 * Identification: src/include/storage/index/ix_manager.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "storage/index/ix_defs.h"
#include "storage/index/ix_extendible_hash_index_handle.h"
#include "storage/index/ix_index_handle.h"
#include "system/sm_meta.h"

namespace easydb {

class IxManager {
 private:
  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;

 public:
  IxManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
      : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {}

  std::string GetIndexName(const std::string &filename, const std::vector<std::string> &index_cols) {
    std::string index_name = filename;
    for (size_t i = 0; i < index_cols.size(); ++i) index_name += "_" + index_cols[i];
    index_name += ".idx";

    return index_name;
  }

  std::string GetIndexName(const std::string &filename, const std::vector<ColMeta> &index_cols) {
    std::string index_name = filename;
    for (size_t i = 0; i < index_cols.size(); ++i) index_name += "_" + index_cols[i].name;
    index_name += ".idx";

    return index_name;
  }

  bool Exists(const std::string &filename, const std::vector<ColMeta> &index_cols) {
    auto ix_name = GetIndexName(filename, index_cols);
    return disk_manager_->IsFile(ix_name);
  }

  bool Exists(const std::string &filename, const std::vector<std::string> &index_cols) {
    auto ix_name = GetIndexName(filename, index_cols);
    return disk_manager_->IsFile(ix_name);
  }

  void CreateIndex(const std::string &filename, const std::vector<ColMeta> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    // Create index file
    disk_manager_->CreateFile(ix_name);
    // Open index file
    int fd = disk_manager_->OpenFile(ix_name);

    // Create file header and write to file
    // Theoretically we have: |page_hdr| + (|attr| + |Rid|) * n <= PAGE_SIZE
    // but we reserve one slot for convenient inserting and deleting, i.e.
    // |page_hdr| + (|attr| + |Rid|) * (n + 1) <= PAGE_SIZE
    int col_tot_len = 0;
    int col_num = index_cols.size();
    for (auto &col : index_cols) {
      col_tot_len += col.len;
    }
    if (col_tot_len > IX_MAX_COL_LEN) {
      throw InvalidColLengthError(col_tot_len);
    }
    // 根据 |page_hdr| + (|attr| + |Rid|) * (n + 1) <= PAGE_SIZE 求得n的最大值btree_order
    // 即 n <= btree_order，那么btree_order就是每个结点最多可插入的键值对数量（实际还多留了一个空位，但其不可插入）
    int btree_order = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(RID)) - 1);
    assert(btree_order > 2);

    // Create file header and write to file
    IxFileHdr *fhdr = new IxFileHdr(IX_NO_PAGE, IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE, col_num, col_tot_len, btree_order,
                                    (btree_order + 1) * col_tot_len, IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
    for (int i = 0; i < col_num; ++i) {
      fhdr->col_types_.push_back(index_cols[i].type);
      fhdr->col_lens_.push_back(index_cols[i].len);
    }
    fhdr->UpdateTotLen();

    char *data = new char[fhdr->tot_len_];
    fhdr->Serialize(data);

    disk_manager_->WritePage(fd, IX_FILE_HDR_PAGE, data, fhdr->tot_len_);

    char page_buf[PAGE_SIZE];  // 在内存中初始化page_buf中的内容，然后将其写入磁盘
    memset(page_buf, 0, PAGE_SIZE);
    // 注意leaf header页号为1，也标记为叶子结点，其前一个/后一个叶子均指向root node
    // Create leaf list header page and write to file
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
      *phdr = {
          .next_free_page_no = IX_NO_PAGE,
          .parent = IX_NO_PAGE,
          .num_key = 0,
          .is_leaf = true,
          .prev_leaf = IX_INIT_ROOT_PAGE,
          .next_leaf = IX_INIT_ROOT_PAGE,
      };
      disk_manager_->WritePage(fd, IX_LEAF_HEADER_PAGE, page_buf, PAGE_SIZE);
    }
    // 注意root node页号为2，也标记为叶子结点，其前一个/后一个叶子均指向leaf header
    // Create root node and write to file
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
      *phdr = {
          .next_free_page_no = IX_NO_PAGE,
          .parent = IX_NO_PAGE,
          .num_key = 0,
          .is_leaf = true,
          .prev_leaf = IX_LEAF_HEADER_PAGE,
          .next_leaf = IX_LEAF_HEADER_PAGE,
      };
      // Must write PAGE_SIZE here in case of future FetchNode()
      disk_manager_->WritePage(fd, IX_INIT_ROOT_PAGE, page_buf, PAGE_SIZE);
    }

    disk_manager_->SetFd2Pageno(fd, IX_INIT_NUM_PAGES - 1);  // DEBUG

    // Close index file
    disk_manager_->CloseFile(fd);

    delete fhdr;
    delete[] data;
  }

  void CreateExtendibleHashIndex(const std::string &filename, const std::vector<ColMeta> &index_cols){
    std::string ix_name = GetIndexName(filename, index_cols);
    // Create index file
    disk_manager_->CreateFile(ix_name);
    // Open index file
    int fd = disk_manager_->OpenFile(ix_name);

    // Create file header and write to file
    // Theoretically we have: |page_hdr| + (|attr| + |rid|) * n <= PAGE_SIZE
    // but we reserve one slot for convenient inserting and deleting, i.e.
    // |page_hdr| + (|attr| + |rid|) * (n + 1) <= PAGE_SIZE
    int col_tot_len = 0;
    int col_num = index_cols.size();
    for (auto &col : index_cols) {
      col_tot_len += col.len;
    }
    if (col_tot_len > IX_MAX_COL_LEN) {
      throw InvalidColLengthError(col_tot_len);
    }

    // Create file header and write to file
    ExtendibleHashIxFileHdr *fhdr =
        new ExtendibleHashIxFileHdr(IX_INIT_HASH_FIRST_FREE_PAGES, IX_INIT_HASH_NUM_PAGES, IX_INIT_DIRECTORY_PAGE,
                                    col_num, col_tot_len, (BUCKET_SIZE + 1) * col_tot_len);
    for (int i = 0; i < col_num; ++i) {
      fhdr->col_types_.push_back(index_cols[i].type);
      fhdr->col_lens_.push_back(index_cols[i].len);
    }
    fhdr->update_tot_len();

    char *data = new char[fhdr->tot_len_];
    fhdr->serialize(data);

    disk_manager_->WritePage(fd, IX_FILE_HDR_PAGE, data, fhdr->tot_len_);
    delete[] data;

    char page_buf[PAGE_SIZE];  // 在内存中初始化page_buf中的内容，然后将其写入磁盘
    memset(page_buf, 0, PAGE_SIZE);

    // Create initial bucket page 0 and write to file
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxExtendibleHashPageHdr *>(page_buf);
      *phdr = {.next_free_page_no = IX_NO_PAGE, .is_valid = true, .local_depth = 1, .key_nums = 0, .size = BUCKET_SIZE};
      // Must write PAGE_SIZE here in case of future fetch_node()
      disk_manager_->WritePage(fd, IX_INIT_BUCKET_0_PAGE, page_buf, PAGE_SIZE);
    }

    // Create initial bucket page 1 and write to file
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxExtendibleHashPageHdr *>(page_buf);
      *phdr = {.next_free_page_no = IX_NO_PAGE, .is_valid = true, .local_depth = 1, .key_nums = 0, .size = BUCKET_SIZE};
      // Must write PAGE_SIZE here in case of future fetch_node()
      disk_manager_->WritePage(fd, IX_INIT_BUCKET_1_PAGE, page_buf, PAGE_SIZE);
    }

    // Create directory bucket page and write to file
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxExtendibleHashPageHdr *>(page_buf);
      *phdr = {
          .next_free_page_no = IX_NO_PAGE, .is_valid = true, .local_depth = -1, .key_nums = 2, .size = BUCKET_SIZE};
      char *tp_keys = page_buf + sizeof(IxExtendibleHashPageHdr);
      RID *tp_rids = reinterpret_cast<RID *>(tp_keys + fhdr->keys_size_);
      // tp_rids[0] = {.page_id_ = IX_INIT_BUCKET_0_PAGE, .slot_num_ = IX_NO_PAGE};  // only page_no is valid
      // tp_rids[1] = {.page_id_ = IX_INIT_BUCKET_1_PAGE, .slot_num_ = IX_NO_PAGE};  // only page_no is valid
      tp_rids[0].Set(IX_INIT_BUCKET_0_PAGE, IX_NO_PAGE);
      tp_rids[1].Set(IX_INIT_BUCKET_1_PAGE, IX_NO_PAGE);
      // Must write PAGE_SIZE here in case of future fetch_node()
      disk_manager_->WritePage(fd, IX_INIT_DIRECTORY_PAGE, page_buf, PAGE_SIZE);
    }
    disk_manager_->SetFd2Pageno(fd, IX_INIT_HASH_NUM_PAGES - 1);  // DEBUG
    delete fhdr;
    // Close index file
    disk_manager_->CloseFile(fd);
  }

  void DestroyIndex(const std::string &filename, const std::vector<ColMeta> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    disk_manager_->DestroyFile(ix_name);
  }

  void DestroyIndex(const std::string &filename, const std::vector<std::string> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    disk_manager_->DestroyFile(ix_name);
  }

  // 注意这里打开文件，创建并返回了index file handle的指针
  std::unique_ptr<IxIndexHandle> OpenIndex(const std::string &filename, const std::vector<ColMeta> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    int fd = disk_manager_->OpenFile(ix_name);
    return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
  }

  std::unique_ptr<IxIndexHandle> OpenIndex(const std::string &filename, const std::vector<std::string> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    int fd = disk_manager_->OpenFile(ix_name);
    return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
  }

  IxExtendibleHashIndexHandle* OpenExtendibleHashIndex(const std::string &filename, const std::vector<ColMeta> &index_cols){
    std::string ix_name = GetIndexName(filename, index_cols);
    int fd = disk_manager_->OpenFile(ix_name);
    IxExtendibleHashIndexHandle *tp = new IxExtendibleHashIndexHandle(disk_manager_, buffer_pool_manager_, fd);
    return tp;
  }

  void CloseIndex(const IxIndexHandle *ih) {
    char *data = new char[ih->file_hdr_->tot_len_];
    ih->file_hdr_->Serialize(data);
    disk_manager_->WritePage(ih->fd_, IX_FILE_HDR_PAGE, data, ih->file_hdr_->tot_len_);
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    buffer_pool_manager_->FlushAllPages(ih->fd_);
    disk_manager_->CloseFile(ih->fd_);
  }

  void CloseExtendibleHashIndex(const IxExtendibleHashIndexHandle* ih){
    char *data = new char[ih->file_hdr_->tot_len_];
    ih->file_hdr_->serialize(data);
    disk_manager_->WritePage(ih->fd_, IX_FILE_HDR_PAGE, data, ih->file_hdr_->tot_len_);
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    buffer_pool_manager_->FlushAllPages(ih->fd_);
    disk_manager_->CloseFile(ih->fd_);
    delete[] data;
  }
  
};

}  // namespace easydb
