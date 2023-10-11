//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);  // TODO(me) ： 这里应该设置为0还是1
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  assert(index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index < GetSize());
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ChildAt(page_id_t child_page_id) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == child_page_id) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetSize());
  return array_[index].second;
}

// 如果页为空会发生什么？——理论上来说一个页不会为空
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, BPT *bpt) -> page_id_t {
  // 采用二分查找：找到大于等于key值的最小值
  int pos = LowerBound(key, bpt);
  if (pos == GetSize()) {
    return array_[GetSize() - 1].second;
  }
  return bpt->CompareKey(array_[pos].first, key) > 0 ? array_[pos - 1].second : array_[pos].second;
}

// 如果该节点为空节点，需要修改第一个位置的指针
// 但是这和另外一种情况冲突了，如果该节点内部只有一个指针（因为删除的原因）就会修改出错
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(page_id_t prev, const KeyType &key, page_id_t next, BPT *bpt) {
  int pos = LowerBound(key, bpt);
  assert(1 <= pos <= GetSize());
  // 所有元素往后挪
  for (int i = GetSize() - 1; i >= pos; i--) {
    array_[i + 1] = array_[i];
  }
  array_[pos] = std::make_pair(key, next);
  IncreaseSize(1);
  if (pos == 1) {
    array_[0].second = prev;
  }
}

// 大于等于key值的最小下标
// return : [1, GetSize()]
// 如果所有元素都小于key，返回GetSize()
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LowerBound(const KeyType &key, BPT *bpt) -> int {
  // 空节点
  if (GetSize() == 1) {
    return 1;
  }
  if (bpt->CompareKey(array_[GetSize() - 1].first, key) < 0) {
    return GetSize();
  }

  int left = 1;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right) / 2;
    if (bpt->CompareKey(array_[mid].first, key) >= 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }
  return left;
}

// 如果TryBreak发生了分裂需要unpin原来的节点和新分裂出来的节点
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::TryBreak(const KeyType &key, BPT *bpt) -> InternalPage * {
  if (GetSize() < GetMaxSize()) {
    return this;
  }

  // 发生分裂
  page_id_t new_internal_page_id;
  Page *new_page = bpt->NewPage(&new_internal_page_id);
  assert(new_page != nullptr);

  auto *new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_internal_page->Init(new_internal_page_id, INVALID_PAGE_ID, bpt->GetInternalMaxSize());

  int split_point = GetMinSize();
  auto split_key = array_[split_point].first;

  // 将原page的一半转移到新page中,并更新新page所有child page 的父节点指针，指向新page
  for (int i = split_point + 1; i < GetSize(); i++) {
    new_internal_page->Insert(array_[i - 1].second, array_[i].first, array_[i].second, bpt);
  }
  new_internal_page->ModifyChildParentPageID(bpt);
  SetSize(GetMinSize());

  auto *parent_page = GetStableParentPage(split_key, bpt);
  parent_page->Insert(GetPageId(), split_key, new_internal_page_id, bpt);
  new_internal_page->SetParentPageId(parent_page->GetPageId());

  bpt->UnpinPage(parent_page->GetPageId(), true);
  if (bpt->CompareKey(split_key, key) < 0) {
    bpt->UnpinPage(GetPageId(), true);
    return new_internal_page;
  }
  bpt->UnpinPage(new_internal_page->GetPageId(), true);
  return this;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ModifyChildParentPageID(BPT *bpt) {
  for (int i = 0; i < GetSize(); i++) {
    // 修改孩子节点的父节点
    Page *child_page = bpt->FetchPage(array_[i].second);
    assert(child_page != nullptr);  // 报错

    auto *page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    assert(page != nullptr);
    page->SetParentPageId(GetPageId());
    bpt->UnpinPage(child_page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ModifyChildParentPageID(page_id_t child_page_id, page_id_t parent_page_id,
                                                             BPT *bpt) {
  // 修改孩子节点的父节点
  Page *child_page = bpt->FetchPage(child_page_id);
  assert(child_page != nullptr);
  auto *page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  assert(page != nullptr);
  page->SetParentPageId(parent_page_id);
  bpt->UnpinPage(child_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetStableParentPage(const KeyType &key, BPT *bpt) -> InternalPage * {
  if (IsRootPage()) {
    return CreateANewParentPage(bpt);
  }
  InternalPage *parent_page = GetParentPage(bpt);
  parent_page = parent_page->TryBreak(key, bpt);

  return parent_page;
}

INDEX_TEMPLATE_ARGUMENTS
// 父节点存在——获取当前节点的父节点
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetParentPage(BPT *bpt) -> InternalPage * {
  page_id_t parent_page_id = GetParentPageId();
  assert(parent_page_id != INVALID_PAGE_ID);  // 这些断言是否是有必要的？

  auto temp_page = bpt->FetchPage(parent_page_id);
  assert(temp_page != nullptr);

  auto *parent_page = reinterpret_cast<InternalPage *>(temp_page->GetData());
  assert(parent_page != nullptr);
  return parent_page;
}

// 父节点不存在——创建一个新的父节点
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CreateANewParentPage(BPT *bpt) -> InternalPage * {
  page_id_t new_root_page_id;
  auto new_page = bpt->NewPage(&new_root_page_id);  // TODO(me): 如果分配内存失败,该如何处理？
  assert(new_page != nullptr);

  auto *new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
  assert(new_internal_page != nullptr);

  new_internal_page->Init(new_root_page_id, INVALID_PAGE_ID, bpt->GetInternalMaxSize());

  SetParentPageId(new_root_page_id);
  bpt->SetRootPageId(new_root_page_id);
  return new_internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &delete_key, BPT *bpt) {
  int pos = LowerBound(delete_key, bpt);
  // 元素不存在
  if (pos == GetSize() || bpt->CompareKey(array_[pos].first, delete_key) != 0) {
    return;
  }

  for (int i = pos + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  SetSize(GetSize() - 1);

  // 不更改父节点，看看会不会有什么影响
  if (GetSize() >= GetMinSize()) {
    return;
  }

  if (IsRootPage()) {
    if (GetSize() == 1) {
      bpt->SetRootPageId(array_[0].second);
      ModifyChildParentPageID(array_[0].second, INVALID_PAGE_ID, bpt);  // 坑
      bpt->DeletePage(GetPageId());
    }
    return;
  }

  InternalPage *parent_page = GetParentPage(bpt);
  page_id_t sabling_page_id;
  bool is_prev;
  KeyType &split_key = parent_page->GetSablingPageId(GetPageId(), sabling_page_id, is_prev, bpt);
  InternalPage *sabling_page = GetSablingPage(sabling_page_id, bpt);

  // 分裂 : 向兄弟节点借一个
  if (sabling_page->GetSize() + GetSize() > GetMaxSize()) {  // 坑：只有大于才分裂
    // 向左兄弟借
    if (is_prev) {
      MappingType last_kv;
      sabling_page->BorrowDataFromLeft(sabling_page->GetSize() - 1, last_kv);
      for (int i = GetSize() - 1; i >= 0; i++) {
        array_[i + 1] = array_[i];
      }
      array_[0].second = last_kv.second;
      int first = 1;
      array_[first].first = split_key;
      split_key = last_kv.first;
      ModifyChildParentPageID(last_kv.second, GetPageId(), bpt);
    } else {
      // 向右兄弟借
      MappingType first_kv;
      sabling_page->BorrowDataFromRight(first_kv);
      Insert(array_[GetSize() - 1].second, split_key, first_kv.second, bpt);
      split_key = first_kv.first;

      // 修改孩子节点的父节点
      // Page *child_page = bpt->FetchPage(first_kv.second);
      // assert(child_page != nullptr);
      // auto *page = reinterpret_cast<BPlusTreePage *>(child_page);
      // assert(page != nullptr);
      // page->SetParentPageId(GetPageId());
      ModifyChildParentPageID(first_kv.second, GetPageId(), bpt);
    }
  } else {
    // 合并
    if (is_prev) {
      AppendPairs(sabling_page, split_key, bpt);
      bpt->DeletePage(GetPageId());
    } else {
      HeadInsertPairs(sabling_page, split_key, bpt);
      bpt->DeletePage(sabling_page->GetPageId());
    }
    parent_page->Remove(split_key, bpt);
  }

  bpt->UnpinPage(parent_page->GetPageId(), true);
  bpt->UnpinPage(sabling_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetSablingPageId(page_id_t page_id, page_id_t &sabling, bool &is_prev, BPT *bpt)
    -> KeyType & {
  int index = 0;
  for (; index < GetSize(); index++) {
    if (array_[index].second == page_id) {
      break;
    }
  }
  // 头部
  if (index == 0) {
    sabling = array_[index + 1].second;
    is_prev = false;
    return array_[index + 1].first;
  }

  // 尾部
  if (index == GetSize() - 1) {
    sabling = array_[index - 1].second;
    is_prev = true;
    return array_[index].first;
  }

  // 对于中间的节点，优先选择可借元素的节点，其次选择左边的节点
  page_id_t pre = array_[index - 1].second;
  page_id_t next = array_[index + 1].second;
  auto *pre_page = reinterpret_cast<BPlusTreePage *>(bpt->FetchPage(pre)->GetData());
  if (pre_page->GetSize() > pre_page->GetMinSize()) {
    sabling = pre;
    is_prev = true;
    bpt->UnpinPage(pre, false);  // 没有进行修改
    return array_[index].first;
  }

  auto *next_page = reinterpret_cast<BPlusTreePage *>(bpt->FetchPage(next)->GetData());
  if (next_page->GetSize() > next_page->GetMinSize()) {
    sabling = next;
    is_prev = false;
    bpt->UnpinPage(pre, false);
    bpt->UnpinPage(next, false);
    return array_[index + 1].first;
  }

  sabling = pre;
  is_prev = true;
  bpt->UnpinPage(pre, false);
  bpt->UnpinPage(next, false);
  return array_[index].first;
}

// 尾部追加
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::AppendPairs(InternalPage *sabling_page, KeyType &split_key, BPT *bpt) {
  sabling_page->Insert(array_[GetSize() - 1].second, split_key, array_[0].second, bpt);
  for (int i = 1; i < GetSize(); i++) {
    sabling_page->Insert(array_[GetSize() - 1].second, array_[i].first, array_[i].second, bpt);
  }
  sabling_page->ModifyChildParentPageID(bpt);
}

// 头部插入
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::HeadInsertPairs(InternalPage *sabling_page, KeyType &split_key, BPT *bpt) {
  // int size = GetSize();
  // int sabling_size = sabling_page->GetSize();
  // for (int i = sabling_size - 1; i >= 0; i--) {
  //   sabling_page->array_[i + size] = sabling_page->array_[i];  // 原来的元素向后挪动
  // }
  // for (int i = 0; i < size; i++) {
  //   sabling_page->array_[i] = array_[i];  // 左边的元素往右挪
  // }
  // sabling_page->array_[size].first = split_key;

  Insert(array_[GetSize() - 1].second, split_key, sabling_page->array_[0].second, bpt);
  for (int i = 1; i < sabling_page->GetSize(); i++) {
    Insert(array_[GetSize() - 1].second, sabling_page->array_[i].first, sabling_page->array_[i].second, bpt);
  }
  ModifyChildParentPageID(bpt);
}

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SwapVriables(InternalPage *sabling_page) {
//   InternalPage temp = *this;
//   *this = *sabling_page;
//   *sabling_page = temp;
// }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowDataFromLeft(int index, MappingType &data) {
  assert(0 < index < GetSize());
  data = array_[index];
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  SetSize(GetSize() - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowDataFromRight(MappingType &data) {
  int first = 1;
  data.first = array_[first].first;
  data.second = array_[0].second;
  array_[0].second = array_[first].second;
  for (int i = 1; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  SetSize(GetSize() - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetSablingPage(page_id_t page_id, BPT *bpt) -> InternalPage * {
  assert(page_id != INVALID_PAGE_ID);
  auto *sabling_page = reinterpret_cast<InternalPage *>(bpt->FetchPage(page_id)->GetData());
  assert(sabling_page != nullptr);
  return sabling_page;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
