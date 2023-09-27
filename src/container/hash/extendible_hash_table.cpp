//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  // 初始化
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_));
  lsb_[dir_[0]] = 0;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;

  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  bool existed = dir_[index]->Find(key, value);
  return existed;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  bool success = dir_[index]->Remove(key);
  return success;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  size_t index = IndexOf(key);
  auto old_bucket = dir_[index];

  // 插入失败
  while (!old_bucket->Insert(key, value)) {
    int conditon = 0;  // 分裂情况 0、local_depth < global_depth  2、local_depth == global_depth
    if (old_bucket->GetDepth() == global_depth_) {
      global_depth_++;
      conditon = 1;
    }
    old_bucket->IncrementDepth();
    num_buckets_ += 1;

    // 创建一个新的bucket
    auto local_depth = old_bucket->GetDepth();
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, local_depth);
    if (num_buckets_ == 2) {
      lsb_[new_bucket] = 1;
    } else {
      lsb_[new_bucket] = (1 << (local_depth - 1)) | lsb_[old_bucket];
    }

    // 桶之间移动数据
    auto data = old_bucket->GetItems();  // data 是old_bucket中数据的引用
    for (const auto &kv : data) {        // 迭代容器的过程中对容器进行更改
      size_t mask = (1 << local_depth) - 1;
      if ((IndexOf(kv.first) & mask) == (lsb_[new_bucket] & mask)) {
        new_bucket->Insert(kv.first, kv.second);
      }
    }
    for (const auto &kv : new_bucket->GetItems()) {
      old_bucket->Remove(kv.first);
    }

    // local_depth < global_depth
    // 只需要更新old_bucket的部分位置
    if (conditon == 0) {
      for (size_t i = 0; i < dir_.size(); i++) {
        size_t mask = (1 << local_depth) - 1;
        if ((i & mask) == (lsb_[new_bucket] & mask)) {
          dir_[i] = new_bucket;
        }
      }
    } else {
      // 所有bucket的位置都需要更新
      std::vector<std::shared_ptr<Bucket>> new_dir(2 * dir_.size());
      for (const auto &[bucket_ptr, lsb] : lsb_) {
        size_t mask = (1 << bucket_ptr->GetDepth()) - 1;
        for (size_t i = 0; i < new_dir.size(); i++) {
          // 如何判断两个数的二进制表示的最后几位是否相等？
          if ((i & mask) == (lsb & mask)) {
            new_dir[i] = bucket_ptr;
          }
        }
      }
      dir_ = new_dir;
    }
    index = IndexOf(key);
    old_bucket = dir_[index];
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &kv : list_) {
    if (kv.first == key) {
      value = kv.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }

  if (size_ == list_.size()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
