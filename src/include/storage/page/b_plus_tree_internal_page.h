//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree;

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  using InternalPage = BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>;
  using BPT = BPlusTree<KeyType, RID, KeyComparator>;
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ChildAt(page_id_t child_page_id) -> int;
  auto ValueAt(int index) const -> ValueType;

  // 需要给外部提供一个查找叶子结点的接口
  auto Lookup(const KeyType &key, BPT *bpt) -> page_id_t;

  // 理论上来说这个函数只能成功不能失败
  // 因为Insert只会向稳定（插入后不会导致分裂）的节点中进行插入
  void Insert(page_id_t prev, const KeyType &key, page_id_t next, BPT *bpt);

  // 试图对中间节点进行分裂，如果不需要分裂直接返回this，否则返回分裂之后的节点用于插入
  auto TryBreak(const KeyType &key, BPT *bpt) -> InternalPage *;

  // 获取“稳定的”父节点（插入后不会导致分裂）
  // 如果没有父节点，创建一个新的父节点
  // 否则尝试对父节点进行分裂，返回一个稳定的父节点
  auto GetStableParentPage(const KeyType &key, BPT *bpt) -> InternalPage *;
  // 父节点存在——获取当前节点的父节点
  auto GetParentPage(BPT *bpt) -> InternalPage *;
  // 父节点不存在——创建一个新的父节点
  auto CreateANewParentPage(BPT *bpt) -> InternalPage *;

  // 修改孩子节点的父节点id
  void ModifyChildParentPageID(BPT *bpt);
  void ModifyChildParentPageID(page_id_t child_page_id, page_id_t parent_page_id, BPT *bpt);

  // 寻找大于等于key值的最小元素的位置
  auto LowerBound(const KeyType &key, BPT *bpt) -> int;

  void Remove(const KeyType &delete_key, BPT *bpt);
  // delete相关helper函数

  // 通过父节点获取当前节点的兄弟节点
  auto GetSablingPageId(page_id_t page_id, page_id_t &sabling, bool &is_prev, BPT *bpt) -> KeyType &;

  // 通过bpt->GetPage来获取兄弟节点
  auto GetSablingPage(page_id_t page_id, BPT *bpt) -> InternalPage *;

  // 从左兄弟借
  void BorrowDataFromLeft(int index, MappingType &data);
  // 从右兄弟借
  void BorrowDataFromRight(MappingType &data);

  // void SwapVriables(InternalPage *sabling_page);

  // 尾部追加
  void AppendPairs(InternalPage *sabling_page, KeyType &split_key, BPT *bpt);
  // 头部插入
  void HeadInsertPairs(InternalPage *sabling_page, KeyType &split_key, BPT *bpt);

  // auto BorrowData(int index, MappingType &data) -> bool;

 private:
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
