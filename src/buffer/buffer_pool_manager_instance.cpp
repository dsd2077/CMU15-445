//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

// #include "buffer_pool_manager_instance.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

void BufferPoolManagerInstance::ResetPage(frame_id_t frame_id, page_id_t page_id) {
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  replacer_->SetEvictable(frame_id, false);  // 这里有一个逻辑bug，pin_count设置为零了，但是又不可驱逐
}

auto BufferPoolManagerInstance::EvictFrame(frame_id_t *frame_id) -> bool {
  bool sucess = replacer_->Evict(frame_id);
  // 驱逐失败，所有页都不可驱逐
  if (!sucess) {
    return false;
  }
  page_id_t evicted_page_id = pages_[*frame_id].GetPageId();
  // 如果替换页为一个脏页，需要将数据写回磁盘
  if (pages_[*frame_id].IsDirty()) {
    disk_manager_->WritePage(evicted_page_id, pages_[*frame_id].GetData());
  }
  page_table_->Remove(evicted_page_id);
  return true;
}

void BufferPoolManagerInstance::SetupNewPage(page_id_t page_id, frame_id_t frame_id) {
  // 分配新的页号
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  // 重置物理块
  ResetPage(frame_id, page_id);
  pages_[frame_id].pin_count_++;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> guard(latch_);
  // 坑3
  // if (free_list_.empty() && (replacer_->Size() == 0)) {
  //   // page_id = nullptr;
  //   return nullptr;
  // }
  if (!free_list_.empty()) {
    // 取出一个物理块号
    frame_id_t frame_id = free_list_.back();
    free_list_.pop_back();
    *page_id = AllocatePage();
    SetupNewPage(*page_id, frame_id);
    return &pages_[frame_id];
  }

  frame_id_t frame_id;
  bool res = EvictFrame(&frame_id);
  if (!res) {
    return nullptr;
  }
  *page_id = AllocatePage();
  SetupNewPage(*page_id, frame_id);
  return &pages_[frame_id];
}

// 如果在页表中没有找到，说明该页不在内存中需要将该页调入内存
// 这个函数跟NewPgImp的区别是什么？————NewPgImp需要通过AllocatePage来分配页号，FetchPgImp的页号由外部给出
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> guard(latch_);
  frame_id_t frame_id;
  // page_id非法检测
  if (page_id < 0) {
    return nullptr;
  }
  bool res = page_table_->Find(page_id, frame_id);
  if (res) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);         // 坑1：忘了最关键的这一步
    replacer_->SetEvictable(frame_id, false);  // 坑2：关键
    return &pages_[frame_id];
  }

  if (!free_list_.empty()) {
    // 取出一个物理块号
    frame_id_t frame_id = free_list_.back();
    free_list_.pop_back();

    SetupNewPage(page_id, frame_id);
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    // return pages_ + frame_id;
    return &pages_[frame_id];
  }

  res = EvictFrame(&frame_id);
  if (!res) {
    return nullptr;
  }
  SetupNewPage(page_id, frame_id);
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> guard(latch_);
  frame_id_t frame_id;
  bool res = page_table_->Find(page_id, frame_id);
  if (!res || pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> guard(latch_);
  frame_id_t frame_id;
  bool res = page_table_->Find(page_id, frame_id);
  if (!res) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    page_id_t page_id = pages_[i].GetPageId();
    if (page_id != INVALID_PAGE_ID) {
      FlushPgImp(page_id);
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> guard(latch_);
  frame_id_t frame_id;
  bool res = page_table_->Find(page_id, frame_id);
  if (!res) {
    return true;
  }
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  // 重置内存以及元数据
  ResetPage(frame_id, INVALID_PAGE_ID);

  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
