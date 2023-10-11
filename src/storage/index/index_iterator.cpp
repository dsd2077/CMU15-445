/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page_ != nullptr) {
    bpt_->UnpinPage(leaf_page_->GetPageId(), false);
  }
}

// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &other) = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPT *bpt, LeafPage *leaf_page, int pos)
    : bpt_(bpt), leaf_page_(leaf_page), current_(pos) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (leaf_page_ == nullptr) {
    return true;
  }
  if (leaf_page_->GetNextPageId() == INVALID_PAGE_ID && current_ >= leaf_page_->GetSize()) {
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (IsEnd()) {
    throw std::runtime_error("operator* error end iterator");
  }
  return leaf_page_->GetItemRef(current_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    throw std::runtime_error("operator++ error : end iterator");
  }
  // 当前节点遍历完，跳至下一个节点
  if (current_ == leaf_page_->GetSize() - 1 && leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
    Page *page = bpt_->FetchPage(leaf_page_->GetNextPageId());
    bpt_->UnpinPage(leaf_page_->GetPageId(), false);

    leaf_page_ = reinterpret_cast<LeafPage *>(page);
    current_ = 0;
  } else {
    current_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
