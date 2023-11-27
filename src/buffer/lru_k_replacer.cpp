//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::EvictFrameFromList(std::list<Frame> &target_list, frame_id_t *frame_id) -> bool {
  auto it = target_list.begin();
  for (; it != target_list.end(); it++) {
    // 可驱逐页
    if (it->evictable_) {
      break;
    }
  }
  if (it == target_list.end()) {
    return false;
  }
  frame_id_t dele_frame_id = it->frame_id_;
  data_.erase(dele_frame_id);
  target_list.erase(it);
  curr_size_--;
  *frame_id = dele_frame_id;
  return true;
}
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> guard(latch_);
  // 如果队列为空return false
  if (data_.empty()) {
    return false;
  }
  // 先从第一个链表中取一个可驱逐的元素，如果没有（要么为空，要么都不可驱逐）
  bool res = EvictFrameFromList(less_than_k_list_, frame_id);
  if (res) {
    return true;
  }
  // 再从第二个链表中取一个可驱逐的元素，如果没有（要么为空，要么都不可驱逐）
  res = EvictFrameFromList(more_than_k_list_, frame_id);
  return res;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::unique_lock<std::mutex> guard(latch_);
  auto it = data_.find(frame_id);
  // 之前没有访问过，没有多余物理页，没有可驱逐页
  if (it == data_.end() && data_.size() == replacer_size_ && curr_size_ == 0) {
    LOG_DEBUG("RecordAccess: no more buffer space!, line:%d", __LINE__);
    throw std::runtime_error("RecordAccess: no more buffer space!");
  }
  // 之前没有访问过，没有多余物理页，有可驱逐页
  if (it == data_.end() && data_.size() == replacer_size_ && curr_size_ > 0) {
    frame_id_t frame_id;
    guard.unlock();
    BUSTUB_ASSERT(Evict(&frame_id), "evit failed");
  }
  // 之前没有访问过，还有多余物理页（可能是刚刚驱逐的）
  if (it == data_.end()) {
    less_than_k_list_.emplace_back(frame_id, 1, false);
    data_[frame_id] = std::make_pair(std::prev(less_than_k_list_.end()), 0);
    return;
  }
  // 之前访问过的页
  it->second.first->visit_cnt_ += 1;
  size_t count = it->second.first->visit_cnt_;
  bool is_evictable = it->second.first->evictable_;
  // 如果在历史队列中，位置不变
  if (it->second.second == 0 && count < k_) {
    return;
  }
  // 从原链表中删除
  if (it->second.second == 0) {
    less_than_k_list_.erase(it->second.first);
  } else {
    more_than_k_list_.erase(it->second.first);
  }
  // 重新插入
  more_than_k_list_.emplace_back(frame_id, count, is_evictable);
  data_[frame_id] = std::make_pair(std::prev(more_than_k_list_.end()), 1);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> guard(latch_);
  auto it = data_.find(frame_id);
  if (it == data_.end()) {
    // LOG_DEBUG("SetEvictable can't find the frame_id line:%d", __LINE__);
    // throw "can't find the frame_id";
    return;
  }

  if (it->second.first->evictable_ && !set_evictable) {  // 之前可驱逐，现在不可驱逐
    curr_size_--;
  } else if (!it->second.first->evictable_ && set_evictable) {  // 之前不可驱逐，现在可驱逐
    curr_size_++;
  }
  it->second.first->evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> guard(latch_);
  auto it = data_.find(frame_id);
  // 无效的frame id
  if (it == data_.end()) {
    return;
  }
  // 不可驱逐页
  if (!it->second.first->evictable_) {
    LOG_DEBUG("Remove can't evictable, line:%d", __LINE__);
    throw std::runtime_error("Remove: can't evictable");
  }

  // 判断it所在链表
  if (it->second.second == 0) {
    less_than_k_list_.erase(it->second.first);
  } else {
    more_than_k_list_.erase(it->second.first);
  }
  data_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
