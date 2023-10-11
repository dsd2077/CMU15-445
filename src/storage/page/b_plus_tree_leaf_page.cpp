//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  assert(index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, BPT *bpt) -> bool {
  // 空节点 or 所有元素小于key值
  int pos = LowerBound(key, bpt);
  if (pos == GetSize()) {
    return false;
  }

  if (bpt->CompareKey(array_[pos].first, key) == 0) {
    value = array_[pos].second;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItemRef(int index) const -> const MappingType & {
  assert(index < GetSize());
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> MappingType {
  assert(index < GetSize());
  return array_[index];
}

// 返回大于等于key值的最小元素的下标
// 如果为空节点则返回0
// 如果没有任何元素大于等于key值，返回GetSize();
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LowerBound(const KeyType &key, BPT *bpt) -> int {
  // 空节点
  if (GetSize() == 0) {
    return 0;
  }
  // 所有元素都小于key
  if (bpt->CompareKey(array_[GetSize() - 1].first, key) < 0) {
    return GetSize();
  }
  int left = 0;
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, BPT *bpt) -> bool {
  // B+树中存在相同的key，插入失败
  ValueType v;
  if (Lookup(key, v, bpt)) {
    return false;
  }
  InsertOne(key, value, bpt);

  // 结点未满，不需要分裂
  if (GetSize() != GetMaxSize()) {
    return true;
  }

  LeafPage *new_leaf_page = BreakDown(bpt);                       // 分裂节点
  InternalPage *new_parent_page = GetStableParentPage(key, bpt);  // 获取父节点

  new_parent_page->Insert(GetPageId(), new_leaf_page->KeyAt(0), new_leaf_page->GetPageId(), bpt);
  new_leaf_page->SetParentPageId(new_parent_page->GetPageId());

  bpt->UnpinPage(new_parent_page->GetPageId(), true);
  bpt->UnpinPage(new_leaf_page->GetPageId(), true);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertOne(const KeyType &key, const ValueType &value, BPT *bpt) {
  int pos = LowerBound(key, bpt);
  // 所有元素往后挪
  for (int i = GetSize() - 1; i >= pos; i--) {
    array_[i + 1] = array_[i];
  }
  array_[pos] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::BreakDown(BPT *bpt) -> LeafPage * {
  page_id_t new_leaf_page_id;
  Page *new_page = bpt->NewPage(&new_leaf_page_id);  // TODO(me): 如果分配内存失败（所有可用内存都已使用），应该怎么办？
  assert(new_page != nullptr);

  auto *new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  assert(new_leaf_page != nullptr);
  new_leaf_page->Init(new_leaf_page_id, INVALID_PAGE_ID, bpt->GetLeafMaxSize());

  int split_point = GetMinSize();  // 分裂点为数组一半的位置

  for (int i = split_point; i < GetSize(); i++) {
    new_leaf_page->Insert(array_[i].first, array_[i].second, bpt);
  }
  SetSize(GetMinSize());

  new_leaf_page->SetNextPageId(GetNextPageId());
  SetNextPageId(new_leaf_page_id);
  return new_leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetStableParentPage(const KeyType &key, BPT *bpt) -> InternalPage * {
  if (IsRootPage()) {
    return CreateANewParentPage(bpt);
  }
  InternalPage *parent_page = GetParentPage(bpt);
  parent_page = parent_page->TryBreak(key, bpt);  // 如果TryBreak发生了分裂，由InternalPage负责另一个节点的Unpin
  return parent_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CreateANewParentPage(BPT *bpt) -> InternalPage * {
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetParentPage(BPT *bpt) -> InternalPage * {
  page_id_t parent_page_id = GetParentPageId();
  assert(parent_page_id != INVALID_PAGE_ID);  // 这些断言是否是有必要的？

  auto temp_page = bpt->FetchPage(parent_page_id);
  assert(temp_page != nullptr);

  auto *parent_page = reinterpret_cast<InternalPage *>(temp_page->GetData());
  assert(parent_page != nullptr);
  return parent_page;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, BPT *bpt) {
  int pos = LowerBound(key, bpt);
  // key does's exist
  if (pos == GetSize() || bpt->CompareKey(key, array_[pos].first) != 0) {
    return;
  }
  // 下面两个方法都不好使
  // memmove(array_+offset, array_+offset+elem_len, (GetSize()-pos-1) * elem_len);
  // std::copy(array_ + offset + 1, array_ + offset + GetSize() - pos, array_ + offset);

  // delete key-value pair in array_
  for (int i = pos + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  SetSize(GetSize() - 1);

  if (IsRootPage()) {
    return;
  }

  InternalPage *parent_page = GetParentPage(bpt);
  // 不分裂,但是要修改父节点
  // TODO(me) : 其实不删除好像也没什么影响，后面可以测试一下
  if (GetSize() >= GetMinSize()) {
    int child_pos = parent_page->ChildAt(GetPageId());
    assert(child_pos != -1);
    if (pos == 0 && child_pos > 0) {
      parent_page->SetKeyAt(child_pos, KeyAt(0));
    }
    return;
  }

  page_id_t sabling_page_id;
  bool is_prev;
  KeyType &split_key = parent_page->GetSablingPageId(GetPageId(), sabling_page_id, is_prev, bpt);
  LeafPage *sabling_page = GetSablingPage(sabling_page_id, bpt);

  // borrow a data from sabling
  if (sabling_page->GetSize() + GetSize() >= GetMaxSize()) {
    // sabling 结点为前驱
    if (is_prev) {
      // delete the last elem in sabling node
      MappingType last_kv;
      sabling_page->BorrowData(sabling_page->GetSize() - 1, last_kv);
      InsertOne(last_kv.first, last_kv.second, bpt);
      // 修改父节点方法一：
      // int pos = parent_page->ChildAt(GetPageId());
      // assert(pos != -1);
      // parent_page->SetKeyAt(pos, last_kv.first);
      // 修改父节点方法二：
      split_key = last_kv.first;
    } else {
      // sabling节点为后继
      MappingType first_kv;
      sabling_page->BorrowData(0, first_kv);
      InsertOne(first_kv.first, first_kv.second, bpt);
      // int pos = parent_page->ChildAt(sabling_page->GetPageId());
      // assert(pos != 0);
      // parent_page->SetKeyAt(pos, sabling_page->KeyAt(0));
      split_key = first_kv.first;
    }
  } else {
    // 往左合并
    if (is_prev) {
      MergeSablingIsPrev(sabling_page, bpt);
      sabling_page->SetNextPageId(GetNextPageId());
      bpt->DeletePage(GetPageId());
    } else {
      MergeSablingIsPost(sabling_page, bpt);
      SetNextPageId(sabling_page->GetNextPageId());
      bpt->DeletePage(sabling_page->GetPageId());
    }
    parent_page->Remove(split_key, bpt);
  }

  bpt->UnpinPage(parent_page->GetPageId(), true);
  bpt->UnpinPage(sabling_page->GetPageId(), true);
}

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_LEAF_PAGE_TYPE::SwapVriables(LeafPage *sabling_page) {
//   LeafPage temp = *this;
//   *this = *sabling_page;
//   *sabling_page = temp;
// }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeSablingIsPrev(LeafPage *sabling_page, BPT *bpt) {
  int size = GetSize();
  int sabling_size = sabling_page->GetSize();
  for (int i = 0; i < size; i++) {
    sabling_page->Insert(array_[i].first, array_[i].second, bpt);
  }
  sabling_page->SetSize(sabling_size + size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeSablingIsPost(LeafPage *sabling_page, BPT *bpt) {
  int size = GetSize();
  int sabling_size = sabling_page->GetSize();
  for (int i = 0; i < sabling_size; i++) {
    Insert(sabling_page->array_[i].first, sabling_page->array_[i].second, bpt);
  }
  SetSize(sabling_size + size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetSablingPage(page_id_t page_id, BPT *bpt) -> LeafPage * {
  assert(page_id != INVALID_PAGE_ID);
  auto *sabling_page = reinterpret_cast<LeafPage *>(bpt->FetchPage(page_id)->GetData());
  assert(sabling_page != nullptr);
  return sabling_page;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::BorrowData(int index, MappingType &data) {
  assert(index < GetSize());
  data = array_[index];
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  SetSize(GetSize() - 1);
}

// TODO(me) :下列代码的作用？
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
