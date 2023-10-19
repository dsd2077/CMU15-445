//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */

// 前向申明BPlusTree类, 这样才能在Insert函数中使用BPT指针
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree;

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using BPT = BPlusTree<KeyType, ValueType, KeyComparator>;

  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;

  // 获取元素的引用，提供给迭代器使用
  auto GetItemRef(int index) const -> const MappingType &;
  auto GetItem(int index) -> MappingType;

  // 查找某个key值的元素是否在叶子结点中
  auto Lookup(const KeyType &key, ValueType &value, BPT *bpt) -> bool;

  // 寻找大于等于key值的最小元素的位置
  auto LowerBound(const KeyType &key, BPT *bpt) -> int;

  // 插入叶结点
  // 插入成功返回true, 否则返回false
  auto Insert(const KeyType &key, const ValueType &value, BPT *bpt, Transaction *transaction) -> bool;
  // InsertOne与Insert相比：1、总是插入(不考虑重复插入)  2、不考虑分裂
  void InsertOne(const KeyType &key, const ValueType &value, BPT *bpt);

  // 叶结点发生分裂
  auto BreakDown(BPT *bpt) -> LeafPage *;

  // 获取“稳定的”父节点（插入后不会导致分裂）
  // 如果没有父节点，创建一个新的父节点
  // 否则尝试对父节点进行分裂，返回一个稳定的父节点
  auto GetStableParentPage(const KeyType &key, BPT *bpt) -> InternalPage *;
  // 父节点存在——获取当前节点的父节点
  auto GetParentPage(BPT *bpt) -> InternalPage *;
  auto GetParentPage(page_id_t parent_page_id, Transaction *transaction) -> InternalPage *;
  // 父节点不存在——创建一个新的父节点
  void CreateANewParentPage(LeafPage *new_leaf_page, BPT *bpt);

  void Remove(const KeyType &key, BPT *bpt, Transaction *transaction);

  auto GetSablingPage(page_id_t page_id, BPT *bpt) -> LeafPage *;
  void BorrowData(int index, MappingType &data);
  // void SwapVriables(LeafPage *sabling_page);
  // 将当前节点中的所有KV pair全部插入到sabling节点中
  void MergeSablingIsPrev(LeafPage *sabling_page, BPT *bpt);
  void MergeSablingIsPost(LeafPage *sabling_page, BPT *bpt);

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[1];  // TODO(me) : 这里的大小固定为1，怎么做到可变大小？为什么要做成这样的可变数组？
};
}  // namespace bustub
