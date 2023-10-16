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
// #include <thread>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, BPT *bpt, Transaction *transaction)
    -> bool {
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

  // 发生分裂
  LeafPage *new_leaf_page = BreakDown(bpt);  // 分裂节点
  // 没有父节点，需要新建一个
  if (IsRootPage()) {
    InternalPage *new_parent_page = CreateANewParentPage(bpt);
    page_id_t parent_page_id =
        new_parent_page->Insert(GetPageId(), new_leaf_page->KeyAt(0), new_leaf_page->GetPageId(), transaction, bpt);
    new_leaf_page->SetParentPageId(parent_page_id);
    bpt->UnpinPage(new_parent_page->GetPageId(), true);  // 新创建的节点不在transaction中，所以需要手动unpin
    // 新创建的父节点没有加锁，别的线程是否有可能拿到该节点？——应该没有可能，因为root_page_latch没有释放
  } else {
    // 发生分裂，并且当前节点不是根节点，则该节点不是安全节点，所以该节点的父节点一定在transaction pageset中
    auto *bpt_internal_page = GetParentPage(GetParentPageId(), transaction);
    // auto *bpt_internal_page = GetParentPage(bpt);
    if (bpt_internal_page == nullptr) {
      throw "bad case";
    }
    assert(bpt_internal_page != nullptr);
    page_id_t parent_page_id =
        bpt_internal_page->Insert(GetPageId(), new_leaf_page->KeyAt(0), new_leaf_page->GetPageId(), transaction, bpt);

    // 如果父节点在插入过程中发生了分裂，可能会导致当前节点的父节点发生改变
    new_leaf_page->SetParentPageId(parent_page_id);
    // bpt->UnpinPage(bpt_internal_page->GetPageId(), true);
  }
  // 父节点和孩子节点都会在transaction中进行unpin
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

  int j = 0;
  for (int i = split_point; i < GetSize(); i++, j++) {
    new_leaf_page->array_[j] = array_[i];
  }
  new_leaf_page->SetSize(j);
  SetSize(GetMinSize());

  new_leaf_page->SetNextPageId(GetNextPageId());
  SetNextPageId(new_leaf_page_id);
  return new_leaf_page;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetStableParentPage(const KeyType &key, BPT *bpt) -> InternalPage * {
//   if (IsRootPage()) {
//     return CreateANewParentPage(bpt);
//   }
//   InternalPage *parent_page = GetParentPage(bpt);
//   parent_page = parent_page->TryBreak(key, bpt);  // 如果TryBreak发生了分裂，由InternalPage负责另一个节点的Unpin
//   return parent_page;
// }

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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetParentPage(page_id_t parent_page_id, Transaction *transaction) -> InternalPage * {
  std::shared_ptr<std::deque<Page *>> ancestors = transaction->GetPageSet();
  for (auto &page : *ancestors) {
    if (page != nullptr && page->GetPageId() == parent_page_id) {
      return reinterpret_cast<InternalPage *>(page->GetData());
    }
  }

  std::cout << "thread " << std::this_thread::get_id() << " looking for " << parent_page_id << std::endl;
  std::cout << "transaction pageset'size = " << ancestors->size() << std::endl;
  std::cout << "the pages exist in transaction are : ";
  for (auto page : *ancestors) {
    if (page != nullptr) {
      std::cout << page->GetPageId() << " ";
    }
  }
  std::cout << std::endl;
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, BPT *bpt) {
  int pos = LowerBound(key, bpt);
  // key does's exist
  if (pos == GetSize() || bpt->CompareKey(key, array_[pos].first) != 0) {
    return;
  }

  // delete key-value pair in array_
  for (int i = pos + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  SetSize(GetSize() - 1);

  if (IsRootPage()) {
    return;
  }

  InternalPage *parent_page = GetParentPage(bpt);
  if (GetSize() >= GetMinSize()) {
    return;
  }

  page_id_t sabling_page_id;
  bool is_prev;
  KeyType &split_key = parent_page->GetSablingPageId(GetPageId(), sabling_page_id, is_prev, bpt);
  Page *buffer_page_sabling = bpt->FetchPage(sabling_page_id);
  assert(buffer_page_sabling != nullptr);
  buffer_page_sabling->WLatch();
  auto *sabling_page = reinterpret_cast<LeafPage *>(buffer_page_sabling->GetData());
  assert(sabling_page != nullptr);

  // borrow a data from sabling
  if (sabling_page->GetSize() + GetSize() >= GetMaxSize()) {
    // sabling 结点为前驱
    if (is_prev) {
      // delete the last elem in sabling node
      MappingType last_kv;
      sabling_page->BorrowData(sabling_page->GetSize() - 1, last_kv);
      InsertOne(last_kv.first, last_kv.second, bpt);
      split_key = last_kv.first;
    } else {
      // sabling节点为后继
      MappingType first_kv;
      sabling_page->BorrowData(0, first_kv);
      InsertOne(first_kv.first, first_kv.second, bpt);
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
  buffer_page_sabling->WUnlatch();
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
    sabling_page->array_[sabling_size] = array_[i];
    sabling_size++;
  }
  sabling_page->SetSize(sabling_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeSablingIsPost(LeafPage *sabling_page, BPT *bpt) {
  int size = GetSize();
  int sabling_size = sabling_page->GetSize();
  for (int i = 0; i < sabling_size; i++) {
    array_[size] = sabling_page->array_[i];
    size++;
  }
  SetSize(size);
}

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetSablingPage(page_id_t page_id, BPT *bpt) -> LeafPage * {
//   assert(page_id != INVALID_PAGE_ID);
//   return sabling_page;
// }

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
