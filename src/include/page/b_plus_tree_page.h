#ifndef MINISQL_B_PLUS_TREE_PAGE_H
#define MINISQL_B_PLUS_TREE_PAGE_H

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

#define UNDEFINED_SIZE 0
/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 28 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | KeySize (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BPlusTreePage {
 public:
  bool IsLeafPage() const;

  bool IsRootPage() const;

  void SetPageType(IndexPageType page_type);

  int GetKeySize() const;

  void SetKeySize(int size);

  int GetSize() const;

  void SetSize(int size);

  void IncreaseSize(int amount);

  int GetMaxSize() const;

  void SetMaxSize(int max_size);

  int GetMinSize() const;

  page_id_t GetParentPageId() const;

  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;

  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

 private:
  // member variable, attributes that both internal and leaf page share
  [[maybe_unused]] IndexPageType page_type_;//中间结点还是叶子结点
  [[maybe_unused]] int key_size_;//当前索引键的长度
  [[maybe_unused]] lsn_t lsn_;//数据页的日志序列号，目前不会用到
  [[maybe_unused]] int size_;//当前结点中存储Key-Value键值对的数量
  [[maybe_unused]] int max_size_;//当前结点最多能够容纳Key-Value键值对的数量
  [[maybe_unused]] page_id_t parent_page_id_;//父结点对应数据页的page_id
  [[maybe_unused]] page_id_t page_id_;//当前结点对应数据页的page_id
};

#endif  // MINISQL_B_PLUS_TREE_PAGE_H
