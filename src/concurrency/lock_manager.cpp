//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 0.检查事务的状态
  auto state = txn->GetState();
  if (state != TransactionState::GROWING && state != TransactionState::SHRINKING) {
    return false;
  }

  // 1.根据隔离级别判断锁请求是否合理
  try {
    IsLegalLockRequest(txn, lock_mode);
  } catch (const TransactionAbortException &) {
    txn->SetState(TransactionState::ABORTED);
    throw;
  }

  // 2.检查锁是否要升级
  if (IsTableLocked(txn, oid)) {
    try {
      return TryUpgradeLock(txn, oid, lock_mode);
    } catch (const TransactionAbortException &) {
      txn->SetState(TransactionState::ABORTED);
      throw;
    }
  }

  auto lr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  table_lock_map_latch_.lock();
  auto iter = table_lock_map_.find(oid);
  if (iter == table_lock_map_.end()) {
    std::shared_ptr<LockRequestQueue> request_queue = std::make_shared<LockRequestQueue>();
    request_queue->request_queue_.emplace_back(lr);
    table_lock_map_[oid] = request_queue;
    table_lock_map_latch_.unlock();

    // 赋予锁
    lr->granted_ = true;
    AddTxnTableLockSet(txn, oid, lock_mode);
    // 记录txn_id ----> Transaction *
    return true;
  }
  std::shared_ptr<LockRequestQueue> request_queue = iter->second;
  std::unique_lock<std::mutex> lock{request_queue->latch_};
  table_lock_map_latch_.unlock();

  // 记录txn_id ----> Transaction *

  request_queue->request_queue_.emplace_back(lr);
  auto lock_request_iter = --request_queue->request_queue_.end();
  while (!Compatible(request_queue, lr) && txn->GetState() != TransactionState::ABORTED) {
    request_queue->cv_.wait(lock);
  }
  // 事务在等待加锁的过程中被外界abort了
  // 由于此时还未获得锁，TransactionManager调用Abort函数也不会清理lr请求，所以需要手动清理并notify_all
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue->request_queue_.erase(lock_request_iter);
    request_queue->cv_.notify_all();
    return false;
  }
  // 通过兼容性检查，赋予锁
  lr->granted_ = true;
  AddTxnTableLockSet(txn, oid, lock_mode);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 1.检查是否持有要解锁的锁
  if (!IsTableLocked(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (HasRowBeingLocked(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  auto iter = table_lock_map_.find(oid);
  std::shared_ptr<LockRequestQueue> request_queue = iter->second;
  std::unique_lock<std::mutex> lock(request_queue->latch_);
  table_lock_map_latch_.unlock();

  auto lock_request_iter = request_queue->request_queue_.begin();
  // 坑：条件写错了
  // while ((*lock_request_iter)->oid_ != oid) {
  //   lock_request_iter++;
  // }
  while ((*lock_request_iter)->txn_id_ != txn->GetTransactionId()) {
    lock_request_iter++;
  }
  auto lock_request = *lock_request_iter;

  request_queue->request_queue_.erase(lock_request_iter);
  request_queue->cv_.notify_all();

  UpdateTransactionState(txn, lock_request);
  DelTxnTableLockSet(txn, lock_request->oid_, lock_request->lock_mode_);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  auto state = txn->GetState();
  if (state != TransactionState::GROWING && state != TransactionState::SHRINKING) {
    return false;
  }
  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (!CheckTableLock(txn, lock_mode, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  // 坑：行锁加锁也要进行升级检查
  if (txn->IsRowExclusiveLocked(oid, rid) || txn->IsRowSharedLocked(oid, rid)) {
    try {
      return TryUpgradeRowLock(txn, oid, lock_mode, rid);
    } catch (const TransactionAbortException &) {
      txn->SetState(TransactionState::ABORTED);
      throw;
    }
  }

  auto lr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  row_lock_map_latch_.lock();
  auto iter = row_lock_map_.find(rid);

  if (iter == row_lock_map_.end()) {
    std::shared_ptr<LockRequestQueue> request_queue = std::make_shared<LockRequestQueue>();
    request_queue->request_queue_.emplace_back(lr);
    row_lock_map_[rid] = request_queue;
    row_lock_map_latch_.unlock();

    // 赋予锁
    lr->granted_ = true;
    AddTxnRowLockSet(txn, oid, lock_mode, rid);

    // 记录txn_id ----> Transaction *
    return true;
  }
  std::shared_ptr<LockRequestQueue> request_queue = iter->second;
  std::unique_lock<std::mutex> lock{request_queue->latch_};
  row_lock_map_latch_.unlock();

  request_queue->request_queue_.emplace_back(lr);
  auto lock_request_iter = --request_queue->request_queue_.end();

  // 记录txn_id ----> Transaction *

  while (!Compatible(request_queue, lr) && txn->GetState() != TransactionState::ABORTED) {
    request_queue->cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue->request_queue_.erase(lock_request_iter);
    request_queue->cv_.notify_all();
    return false;
  }
  // 通过兼容性检查，赋予锁
  lr->granted_ = true;
  AddTxnRowLockSet(txn, oid, lock_mode, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  if (!(txn->IsRowExclusiveLocked(oid, rid) || txn->IsRowSharedLocked(oid, rid))) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  row_lock_map_latch_.lock();
  auto iter = row_lock_map_.find(rid);
  std::shared_ptr<LockRequestQueue> request_queue = iter->second;
  std::unique_lock<std::mutex> lock(request_queue->latch_);
  row_lock_map_latch_.unlock();

  auto lock_request_iter = request_queue->request_queue_.begin();
  while (lock_request_iter != request_queue->request_queue_.end()) {
    if ((*lock_request_iter)->rid_ == rid) {
      break;
    }
    lock_request_iter++;
  }
  auto lock_request = *lock_request_iter;

  request_queue->request_queue_.erase(lock_request_iter);
  request_queue->cv_.notify_all();

  UpdateTransactionState(txn, lock_request);
  DelTxnRowLockSet(txn, lock_request->oid_, lock_request->lock_mode_, rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_[t1] = {t2};
  } else {
    waits_for_[t1].emplace_back(t2);
  }
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  auto iter = waits_for_.find(t1);
  if (iter == waits_for_.end()) {
    return;
  }
  auto wait_for_list = iter->second;
  wait_for_list.erase(std::remove(wait_for_list.begin(), wait_for_list.end(), t2), wait_for_list.end());
  waits_for_latch_.unlock();
}

auto LockManager::Dfs(txn_id_t node, std::unordered_set<txn_id_t> &visited, std::unordered_set<txn_id_t> &rec_stack,
                      txn_id_t *txn_id) -> bool {
  if (rec_stack.find(node) != rec_stack.end()) {
    *txn_id = *std::max_element(rec_stack.begin(), rec_stack.end());
    return true;  // 发现环
  }
  if (visited.find(node) != visited.end()) {
    return false;  // 已访问，但未发现环
  }

  visited.insert(node);
  rec_stack.insert(node);
  std::sort(waits_for_[node].begin(), waits_for_[node].end());
  for (txn_id_t neighbor : waits_for_[node]) {
    if (Dfs(neighbor, visited, rec_stack, txn_id)) {
      return true;
    }
  }

  rec_stack.erase(node);
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_set<txn_id_t> visited;
  std::unordered_set<txn_id_t> rec_stack;
  std::vector<txn_id_t> wait_txns;
  // 对key进行排序
  for (auto &pair : waits_for_) {
    wait_txns.emplace_back(pair.first);
  }
  std::sort(wait_txns.begin(), wait_txns.end());

  for (auto txn : wait_txns) {
    if (Dfs(txn, visited, rec_stack, txn_id)) {
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  waits_for_latch_.lock();
  for (auto &[t1, wait_list] : waits_for_) {
    for (auto &t2 : wait_list) {
      edges.emplace_back(std::make_pair(t1, t2));
    }
  }
  waits_for_latch_.unlock();
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    // 检测图中是否有环，如果有环选一个倒霉蛋把它Abort掉
    std::lock_guard<std::mutex> table_lock(table_lock_map_latch_);
    std::lock_guard<std::mutex> wait_for_lock(waits_for_latch_);

    std::unordered_map<txn_id_t, std::vector<table_oid_t>> txn_wait_tables;  // 每一个等待事务，在等待哪张表
    for (const auto &[table_id, request_que] : table_lock_map_) {
      // 队列中没有等待事务
      if (request_que->request_queue_.empty() || request_que->request_queue_.back()->granted_) {
        continue;
      }
      std::vector<txn_id_t> locked_txn;                            // 已经加锁的事务
      auto locked_end_iter = request_que->request_queue_.begin();  // 指向请求队列中等待加锁的事务
      while (locked_end_iter != request_que->request_queue_.end() && (*locked_end_iter)->granted_) {
        locked_txn.emplace_back((*locked_end_iter)->txn_id_);
        locked_end_iter++;
      }
      for (auto iter = locked_end_iter; iter != request_que->request_queue_.end(); iter++) {
        if (txn_wait_tables.find((*iter)->txn_id_) != txn_wait_tables.end()) {
          txn_wait_tables[(*iter)->txn_id_].emplace_back(table_id);
        } else {
          txn_wait_tables[(*iter)->txn_id_] = std::vector<table_oid_t>{table_id};
        }
        if (waits_for_.find((*iter)->txn_id_) == waits_for_.end()) {
          waits_for_[(*iter)->txn_id_] = locked_txn;  // TODO(dsd) : 死锁检测算法中的边是不是这么算的？
        } else {
          waits_for_[(*iter)->txn_id_].insert(waits_for_[(*iter)->txn_id_].end(), locked_txn.begin(), locked_txn.end());
        }
      }
    }

    std::lock_guard<std::mutex> row_lock(row_lock_map_latch_);
    // 每一个等待事务，在等待哪一行
    std::unordered_map<txn_id_t, std::vector<RID>> txn_wait_rows;
    for (const auto &[rid, request_que] : row_lock_map_) {
      // 队列中没有等待事务
      if (request_que->request_queue_.empty() || request_que->request_queue_.back()->granted_) {
        continue;
      }
      std::vector<txn_id_t> locked_txn;
      auto locked_end_iter = request_que->request_queue_.begin();
      while (locked_end_iter != request_que->request_queue_.end() && (*locked_end_iter)->granted_) {
        locked_txn.emplace_back((*locked_end_iter)->txn_id_);
        locked_end_iter++;
      }
      for (auto iter = locked_end_iter; iter != request_que->request_queue_.end(); iter++) {
        if (txn_wait_rows.find((*iter)->txn_id_) != txn_wait_rows.end()) {
          txn_wait_rows[(*iter)->txn_id_].emplace_back(rid);
        } else {
          txn_wait_rows[(*iter)->txn_id_] = std::vector<RID>{rid};
        }
        if (waits_for_.find((*iter)->txn_id_) == waits_for_.end()) {
          waits_for_[(*iter)->txn_id_] = locked_txn;
        } else {
          waits_for_[(*iter)->txn_id_].insert(waits_for_[(*iter)->txn_id_].end(), locked_txn.begin(), locked_txn.end());
        }
      }
    }
    txn_id_t youngest_txn;
    // 如果有环，将最年轻的事务Abort掉
    if (HasCycle(&youngest_txn)) {
      // assert(txns_.find(youngest_txn) != txns_.end());
      TransactionManager::GetTransaction(youngest_txn)->SetState(TransactionState::ABORTED);
      // 找到该事务所阻塞的表队列，将队列唤醒
      auto table_iter = txn_wait_tables.find(youngest_txn);
      if (table_iter != txn_wait_tables.end()) {
        for (auto table_id : table_iter->second) {
          auto request_que = table_lock_map_[table_id];
          request_que->cv_.notify_all();
        }
      }

      // 找到该事务所阻塞的行队列，将队列唤醒
      auto row_iter = txn_wait_rows.find(youngest_txn);
      if (row_iter != txn_wait_rows.end()) {
        for (auto &rid : row_iter->second) {
          auto request_que = row_lock_map_[rid];
          request_que->cv_.notify_all();
        }
      }
    }

    waits_for_.clear();
  }
}

//////////////////////////////////////////////
// private helper functions
auto LockManager::IsLegalLockRequest(Transaction *txn, LockMode lock_mode) -> bool {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      // 在 REPEATABLE_READ 隔离级别下，所有锁在生长状态下都允许，但在收缩状态下不允许任何锁
      if (txn->GetState() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // 在 READ_COMMITTED 隔离级别下，所有锁在生长状态下都允许，但在收缩状态下只允许 IS 和 S 锁
      if (txn->GetState() == TransactionState::SHRINKING &&
          !(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // 在 READ_UNCOMMITTED 隔离级别下，只允许 IX 和 X 锁，且不允许 S/IS/SIX 锁
      if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
  }
  return true;
}

auto LockManager::IsTableLocked(Transaction *txn, const table_oid_t &oid) -> bool {
  return txn->IsTableIntentionSharedLocked(oid) || txn->IsTableSharedLocked(oid) ||
         txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
         txn->IsTableSharedIntentionExclusiveLocked(oid);
}

// 同一个事务可能对一张表加多个锁吗？————不可能
auto LockManager::GetTableLockMode(Transaction *txn, const table_oid_t &oid) -> LockMode {
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return LockMode::INTENTION_SHARED;
  }
  if (txn->IsTableSharedLocked(oid)) {
    return LockMode::SHARED;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return LockMode::INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableExclusiveLocked(oid)) {
    return LockMode::EXCLUSIVE;
  }
  return LockMode::SHARED_INTENTION_EXCLUSIVE;
}

auto LockManager::IsTableBeingUpgrading(const table_oid_t &oid) -> bool {
  std::lock_guard<std::mutex> lock(table_lock_map_latch_);
  auto iter = table_lock_map_.find(oid);
  assert(iter != table_lock_map_.end());
  return iter->second->upgrading_ != INVALID_TXN_ID;
}

auto LockManager::IsRowBeingUpgrading(const RID &rid) -> bool {
  std::lock_guard<std::mutex> lock(row_lock_map_latch_);
  auto iter = row_lock_map_.find(rid);
  assert(iter != row_lock_map_.end());
  return iter->second->upgrading_ != INVALID_TXN_ID;
}

auto LockManager::TryUpgradeLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) -> bool {
  LockMode current_lock_mode = GetTableLockMode(txn, oid);
  // TODO(dsd) : 当一个事务有更高级别的锁时，是不是可以不用加锁了？
  // 当一个事务拥有更高级的锁，再去加低级的锁时，要抛出异常，也就是只能锁升级不能降级。为什么？
  // if (HasHigherLevelLock(current_lock_mode, lock_mode)) {
  //   return true;
  // }
  if (current_lock_mode == lock_mode) {
    return true;
  }
  // Check for valid lock upgrade paths
  try {
    CheckValidUpgrade(txn, current_lock_mode, lock_mode);
  } catch (const TransactionAbortException &) {
    throw;
  }
  // Ensure only one transaction is upgrading its lock at a time
  if (IsTableBeingUpgrading(oid)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // Perform the lock upgrade
  return UpgradeLock(txn, oid, lock_mode);
}

void LockManager::CheckValidUpgrade(Transaction *txn, LockMode current_lock_mode, LockMode lock_mode) {
  switch (current_lock_mode) {
    case LockMode::INTENTION_SHARED:
      break;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_mode != LockMode::EXCLUSIVE) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::EXCLUSIVE:
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      break;
  }
}

void LockManager::AddTxnTableLockSet(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->emplace(oid);
      break;
  }
}

void LockManager::DelTxnTableLockSet(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
}

auto LockManager::Compatible(std::shared_ptr<LockRequestQueue> lock_request_queue,
                             std::shared_ptr<LockRequest> current_request) -> bool {
  auto is_compatible = [this, &lock_request_queue,
                        &current_request](const std::function<bool(LockMode)> &checkCondition) {
    for (auto &request : lock_request_queue->request_queue_) {
      if (request == current_request) {
        break;
      }
      if (!request->granted_ && !Compatible(lock_request_queue, request)) {
        return false;
      }
      if (!checkCondition(request->lock_mode_)) {
        return false;
      }
    }
    return true;
  };
  switch (current_request->lock_mode_) {
    case LockMode::INTENTION_SHARED:
      // 与排他锁不兼容
      return is_compatible([](LockMode mode) { return mode != LockMode::EXCLUSIVE; });
    case LockMode::INTENTION_EXCLUSIVE:
      // 仅与意向共享锁或意向排他锁兼容
      return is_compatible(
          [](LockMode mode) { return mode == LockMode::INTENTION_SHARED || mode == LockMode::INTENTION_EXCLUSIVE; });
    case LockMode::SHARED:
      // 仅与共享锁或意向共享锁兼容
      return is_compatible(
          [](LockMode mode) { return mode == LockMode::INTENTION_SHARED || mode == LockMode::SHARED; });
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      // 仅与意向共享锁兼容
      return is_compatible([](LockMode mode) { return mode == LockMode::INTENTION_SHARED; });
    case LockMode::EXCLUSIVE:
      // 仅当队列中没有其他请求时兼容
      return lock_request_queue->request_queue_.front() == current_request;
    default:
      // 未知的锁模式
      return false;
  }
}

auto LockManager::UpgradeLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode, bool is_row, const RID &rid)
    -> bool {
  // 在请求队列中找到之前的请求
  std::shared_ptr<LockRequestQueue> request_queue;
  std::unique_lock<std::mutex> request_queue_lock;
  if (is_row) {
    row_lock_map_latch_.lock();
    auto iter = row_lock_map_.find(rid);
    assert(iter != row_lock_map_.end());
    request_queue = iter->second;
    std::unique_lock<std::mutex> lock{request_queue->latch_};
    request_queue_lock.swap(lock);
    row_lock_map_latch_.unlock();
  } else {
    table_lock_map_latch_.lock();
    auto iter = table_lock_map_.find(oid);
    assert(iter != table_lock_map_.end());
    request_queue = iter->second;
    std::unique_lock<std::mutex> lock{request_queue->latch_};
    request_queue_lock.swap(lock);
    table_lock_map_latch_.unlock();
  }

  auto prev_req = request_queue->request_queue_.begin();
  while ((*prev_req)->txn_id_ != txn->GetTransactionId()) {
    prev_req++;
  }
  // 删除之前的请求
  if (is_row) {
    DelTxnRowLockSet(txn, (*prev_req)->oid_, (*prev_req)->lock_mode_, rid);
  } else {
    DelTxnTableLockSet(txn, (*prev_req)->oid_, (*prev_req)->lock_mode_);
  }
  request_queue->request_queue_.erase(prev_req);
  std::shared_ptr<bustub::LockManager::LockRequest> lr;
  if (is_row) {
    lr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  } else {
    lr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  }
  auto proper_pos = request_queue->request_queue_.begin();
  while (proper_pos != request_queue->request_queue_.end() && (*proper_pos)->granted_) {
    proper_pos++;
  }
  request_queue->request_queue_.insert(proper_pos, lr);
  request_queue->upgrading_ = txn->GetTransactionId();
  auto lock_request_iter = --request_queue->request_queue_.end();

  while (!Compatible(request_queue, lr) && txn->GetState() != TransactionState::ABORTED) {
    request_queue->cv_.wait(request_queue_lock);
  }
  // 事务在等待加锁的过程中被外界abort了
  // 由于此时还未获得锁，TransactionManager调用Abort函数也不会清理lr请求，所以需要手动清理并notify_all
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue->request_queue_.erase(lock_request_iter);
    request_queue->upgrading_ = INVALID_TXN_ID;
    request_queue->cv_.notify_all();
    return false;
  }
  // 通过兼容性检查，赋予锁
  lr->granted_ = true;
  if (is_row) {
    AddTxnRowLockSet(txn, oid, lock_mode, rid);
  } else {
    AddTxnTableLockSet(txn, oid, lock_mode);
  }
  request_queue->upgrading_ = INVALID_TXN_ID;
  return true;
}

auto LockManager::HasRowBeingLocked(Transaction *txn, const table_oid_t &oid) -> bool {
  auto iter1 = txn->GetExclusiveRowLockSet()->find(oid);
  if (iter1 != txn->GetExclusiveRowLockSet()->end() && !iter1->second.empty()) {
    return true;
  }
  auto iter2 = txn->GetSharedRowLockSet()->find(oid);
  return iter2 != txn->GetSharedRowLockSet()->end() && !iter2->second.empty();
}

void LockManager::UpdateTransactionState(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  if (txn == nullptr || lock_request == nullptr || txn->GetState() != TransactionState::GROWING) {
    return;
  }
  // TODO(dsd) :这里可能有问题
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      // In REPEATABLE_READ, unlocking either S or X locks sets state to SHRINKING
      txn->SetState(TransactionState::SHRINKING);
      break;
    case IsolationLevel::READ_UNCOMMITTED:
    case IsolationLevel::READ_COMMITTED:
      // In READ_COMMITTED, only unlocking X locks sets state to SHRINKING
      if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
  }
}

auto LockManager::CheckTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      if (txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        return true;
      }
      return false;
    case LockMode::SHARED:
      if (IsTableLocked(txn, oid)) {
        return true;
      }
      return false;
    default:
      return false;
  }
}

void LockManager::AddTxnRowLockSet(Transaction *txn, const table_oid_t &oid, LockMode lock_mode, const RID &rid) {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].emplace(rid);
      break;
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].emplace(rid);
      break;
    default:
      break;
  }
}

void LockManager::DelTxnRowLockSet(Transaction *txn, const table_oid_t &oid, LockMode lock_mode, const RID &rid) {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
      if ((*txn->GetExclusiveRowLockSet())[oid].empty()) {
        txn->GetExclusiveRowLockSet()->erase(oid);
      }
      break;
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].erase(rid);
      if ((*txn->GetSharedRowLockSet())[oid].empty()) {
        txn->GetSharedRowLockSet()->erase(oid);
      }
      break;
    default:
      break;
  }
}

auto LockManager::HasHigherLevelLock(LockMode current_lock_mode, LockMode lock_mode) -> bool {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      return true;
    case LockMode::SHARED:
      if (current_lock_mode == LockMode::SHARED || current_lock_mode == LockMode::EXCLUSIVE ||
          current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (current_lock_mode == LockMode::INTENTION_EXCLUSIVE || current_lock_mode == LockMode::EXCLUSIVE ||
          current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || current_lock_mode == LockMode::EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::EXCLUSIVE:
      if (current_lock_mode == LockMode::EXCLUSIVE) {
        return true;
      }
  }
  return false;
}
auto LockManager::TryUpgradeRowLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode, const RID &rid)
    -> bool {
  LockMode current_lock_mode;
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    current_lock_mode = LockMode::EXCLUSIVE;
  } else {
    current_lock_mode = LockMode::SHARED;
  }

  if (current_lock_mode == lock_mode) {
    return true;
  }
  // Check for valid lock upgrade paths
  try {
    CheckValidUpgrade(txn, current_lock_mode, lock_mode);
  } catch (const TransactionAbortException &) {
    throw;
  }

  // Ensure only one transaction is upgrading its lock at a time
  if (IsRowBeingUpgrading(rid)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // Perform the lock upgrade
  return UpgradeLock(txn, oid, lock_mode, true, rid);
}

}  // namespace bustub
