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
  if (bpt_page_ != nullptr) {
    buffer_page_->RUnlatch();
    bpt_->UnpinPage(bpt_page_->GetPageId(), false);
  }
}

// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &other) = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPT *bpt, Page *buffer_page, LeafPage *bpt_page, int pos)
    : bpt_(bpt), buffer_page_(buffer_page), bpt_page_(bpt_page), current_(pos) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (bpt_page_ == nullptr) {
    return true;
  }
  if (bpt_page_->GetNextPageId() == INVALID_PAGE_ID && current_ >= bpt_page_->GetSize()) {
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (IsEnd()) {
    throw std::runtime_error("operator* error end iterator");
  }
  return bpt_page_->GetItemRef(current_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    throw std::runtime_error("operator++ error : end iterator");
  }
  // 当前节点遍历完，跳至下一个节点
  if (current_ == bpt_page_->GetSize() - 1 && bpt_page_->GetNextPageId() != INVALID_PAGE_ID) {
    // 拿到下一个节点并加锁
    Page *next_page = bpt_->FetchPage(bpt_page_->GetNextPageId());
    next_page->RLatch();

    // 将当前节点解锁
    buffer_page_->RUnlatch();
    bpt_->UnpinPage(bpt_page_->GetPageId(), false);

    // 调至下一个节点
    buffer_page_ = next_page;
    bpt_page_ = reinterpret_cast<LeafPage *>(next_page);
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
