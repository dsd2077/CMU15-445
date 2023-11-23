//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using BPT = BPlusTree<KeyType, ValueType, KeyComparator>;

  // you may define your own constructor based on your member variables
  IndexIterator() = default;
  IndexIterator(BPT *bpt, Page *buffer_page, LeafPage *bpt_page, int pos);
  // 复制构造函数
  IndexIterator(const IndexIterator &other) = default;
  auto operator=(const IndexIterator &other) -> IndexIterator & {
    // 检查自赋值
    if (this == &other) {
      return *this;
    }
    // 复制数据
    bpt_ = other.bpt_;
    if (other.buffer_page_ != nullptr) {
      buffer_page_ = bpt_->FetchPage(other.buffer_page_->GetPageId());
      buffer_page_->RLatch();
      bpt_page_ = reinterpret_cast<LeafPage *>(buffer_page_);
    }
    current_ = other.current_;
    // 返回当前对象的引用
    return *this;
  }

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  // TODO(me) : 为什么这两个函数的实现放在头文件中？
  auto operator==(const IndexIterator &itr) const -> bool {
    return (this->bpt_page_ == itr.bpt_page_ && this->current_ == itr.current_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return (this->bpt_page_ != itr.bpt_page_ || this->current_ != itr.current_);
  }

 private:
  // add your own private member variables here
  BPT *bpt_ = nullptr;
  Page *buffer_page_ = nullptr;
  LeafPage *bpt_page_ = nullptr;
  int current_;
};

}  // namespace bustub
