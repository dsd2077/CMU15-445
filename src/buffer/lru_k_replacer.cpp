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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::EvictFrameFromList(std::list<std::tuple<frame_id_t, int, bool>> &target_list, frame_id_t *frame_id)
    -> bool {
  auto it = target_list.begin();
  for (; it != target_list.end(); it++) {
    // 可驱逐页
    if (std::get<2>(*it)) {
      break;
    }
  }
  if (it == target_list.end()) {
    return false;
  }
  frame_id_t dele_frame_id = std::get<0>(*it);
  data_.erase(dele_frame_id);
  target_list.erase(it);
  curr_size_--;
  *frame_id = dele_frame_id;
  return true;
}
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> guard(latch_);
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
  std::scoped_lock<std::mutex> guard(latch_);
  // BUSTUB_ASSERT((frame_id < 0 || frame_id > replacer_size_), "invald address");

  auto it = data_.find(frame_id);
  // 之前没有访问过并且还有多余物理页
  // 之前没有访问过，没有多余物理页，没有可驱逐页
  if (it == data_.end() && data_.size() == replacer_size_ && curr_size_ == 0) {
    return;
  }
  // 之前没有访问过，没有多余物理页，有可驱逐页
  if (it == data_.end() && data_.size() == replacer_size_ && curr_size_ > 0) {
    frame_id_t temp_frame_it;
    Evict(&temp_frame_it);
  }
  // 之前没有访问过，还有多余物理页（可能是刚刚驱逐的）
  if (it == data_.end()) {
    less_than_k_list_.emplace_back(frame_id, 1, false);
    data_[frame_id] = std::make_pair(std::prev(less_than_k_list_.end()), 0);

    return;
  }
  // 之前访问过的页
  std::get<1>(*(it->second.first)) += 1;
  size_t count = std::get<1>(*(it->second.first));
  bool is_evictable = std::get<2>(*(it->second.first));
  // 历史队列中的数据为第一次访问时的位置，只要未达到k次访问频率，位置一直保持不变。
  if (it->second.second == 0 && count < k_) {
    return;
  }
  // 从原链表中删除
  if (it->second.second == 0 && count >= k_) {
    less_than_k_list_.erase(it->second.first);
  } else {
    more_than_k_list_.erase(it->second.first);
  }
  // 重新插入
  if (count >= k_) {
    more_than_k_list_.emplace_back(frame_id, count, is_evictable);
    data_[frame_id] = std::make_pair(std::prev(more_than_k_list_.end()), 1);
  } else {
    less_than_k_list_.emplace_back(frame_id, count, is_evictable);
    data_[frame_id] = std::make_pair(std::prev(less_than_k_list_.end()), 0);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> guard(latch_);
  auto it = data_.find(frame_id);
  if (it == data_.end()) {
    return;
  }
  if (std::get<2>(*(it->second.first)) && !set_evictable) {
    curr_size_--;
  } else if (!std::get<2>(*(it->second.first)) && set_evictable) {
    curr_size_++;
  }
  std::get<2>(*(it->second.first)) = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> guard(latch_);
  auto it = data_.find(frame_id);
  // 未找到直接返回
  if (it == data_.end()) {
    return;
  }
  // 不可驱逐页
  if (!std::get<2>(*(it->second.first))) {
    // throw an exception
    throw std::runtime_error("Remove: invald frame id");
    return;
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
  std::scoped_lock<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
