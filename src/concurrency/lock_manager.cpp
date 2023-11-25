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

// 当返回false时，外部要如何处理？
// 1.循环加锁
// 2.抛出异常
// 无论是哪一种情况，都不能再继续运行下去
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 0.检查事务的状态
  if (txn->GetState() != TransactionState::GROWING && txn->GetState() != TransactionState::SHRINKING) {
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  // 1.根据隔离级别判断锁请求是否合理
  try {
    IsLegalLockRequest(txn, lock_mode);
  } catch (const TransactionAbortException &) {
    txn->SetState(TransactionState::ABORTED);
    throw;
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  bool upgrading = false;
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        LOG_DEBUG("exception LockTable queue is updateing line:%d", __LINE__);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      // Check for valid lock upgrade paths
      if (!CheckValidUpgrade(txn, request->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        LOG_DEBUG("exception LockTable incompatible upgrade line:%d", __LINE__);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(request);
      DelTxnTableLockSet(txn, request->oid_, request->lock_mode_);

      auto proper_pos = lock_request_queue->request_queue_.begin();
      while (proper_pos != lock_request_queue->request_queue_.end() && (*proper_pos)->granted_) {
        proper_pos++;
      }
      lock_request_queue->request_queue_.insert(proper_pos, lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      upgrading = true;
      break;
    }
  }
  if (!upgrading) {
    lock_request_queue->request_queue_.emplace_back(lock_request);
  }
  while (!Compatible(lock_request_queue, lock_request)) {
    lock_request_queue->cv_.wait(lock);
    // 事务在等待加锁的过程中被外界abort了
    // 由于此时还未获得锁，TransactionManager调用Abort函数也不会清理lr请求，所以需要手动清理并notify_all
    if (txn->GetState() == TransactionState::ABORTED) {
      if (upgrading) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  if (upgrading) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  lock_request->granted_ = true;
  AddTxnTableLockSet(txn, oid, lock_mode);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 1.检查是否持有要解锁的锁
  if (!IsTableLocked(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    LOG_DEBUG("exception UnlockTable AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD line:%d", __LINE__);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (HasRowBeingLocked(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    LOG_DEBUG("exception UnlockTable AbortReason::表中还有行锁，不能解锁 line:%d", __LINE__);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  txn->UnlockTxn();

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
  // while ((*lock_request_iter)->txn_id_ != txn->GetTransactionId()) {
  //   lock_request_iter++;
  // }
  while (lock_request_iter != request_queue->request_queue_.end()) {
    if ((*lock_request_iter)->txn_id_ == txn->GetTransactionId()) {
      break;
    }
    lock_request_iter++;
  }
  if (lock_request_iter == request_queue->request_queue_.end()) {
    LOG_DEBUG("exception UnlockTable AbortReason::没找到要解锁的表 line:%d", __LINE__);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request = *lock_request_iter;

  UpdateTransactionState(txn, lock_request);
  DelTxnTableLockSet(txn, lock_request->oid_, lock_request->lock_mode_);
  request_queue->request_queue_.erase(lock_request_iter);
  request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() != TransactionState::GROWING && txn->GetState() != TransactionState::SHRINKING) {
    return false;
  }
  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    LOG_DEBUG("exception LockRow AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW line:%d", __LINE__);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (!CheckTableLock(txn, lock_mode, oid)) {
    txn->SetState(TransactionState::ABORTED);
    LOG_DEBUG("exception LockRow AbortReason::TABLE_LOCK_NOT_PRESENT line:%d", __LINE__);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  // 坑：即便是行锁也要进行检查
  try {
    IsLegalLockRequest(txn, lock_mode);
  } catch (const TransactionAbortException &) {
    txn->SetState(TransactionState::ABORTED);
    throw;
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  std::unique_lock<std::mutex> lock{lock_request_queue->latch_};
  row_lock_map_latch_.unlock();

  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  bool upgrading = false;
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        LOG_DEBUG("exception LockRow AbortReason::UPGRADE_CONFLICT line:%d", __LINE__);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      // Check for valid lock upgrade paths
      if (!CheckValidUpgrade(txn, request->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        LOG_DEBUG("exception LockRow AbortReason::INCOMPATIBLE_UPGRADE line:%d", __LINE__);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(request);
      DelTxnRowLockSet(txn, request->oid_, request->lock_mode_, rid);

      auto proper_pos = lock_request_queue->request_queue_.begin();
      while (proper_pos != lock_request_queue->request_queue_.end() && (*proper_pos)->granted_) {
        proper_pos++;
      }
      lock_request_queue->request_queue_.insert(proper_pos, lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      upgrading = true;
      break;
    }
  }
  if (!upgrading) {
    lock_request_queue->request_queue_.emplace_back(lock_request);
  }
  while (!Compatible(lock_request_queue, lock_request)) {
    lock_request_queue->cv_.wait(lock);
    // 事务在等待加锁的过程中被外界abort了
    // 由于此时还未获得锁，TransactionManager调用Abort函数也不会清理lr请求，所以需要手动清理并notify_all
    if (txn->GetState() == TransactionState::ABORTED) {
      if (upgrading) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  if (upgrading) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  lock_request->granted_ = true;
  AddTxnRowLockSet(txn, oid, lock_mode, rid);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  txn->LockTxn();
  if (!(txn->IsRowExclusiveLocked(oid, rid) || txn->IsRowSharedLocked(oid, rid))) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    LOG_DEBUG("exception UnlockRow AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD line:%d", __LINE__);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  txn->UnlockTxn();

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    LOG_DEBUG("exception UnlockRow AbortReason::没找到要解锁的行 line:%d", __LINE__);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<LockRequestQueue> request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(request_queue->latch_);
  row_lock_map_latch_.unlock();

  auto lock_request_iter = request_queue->request_queue_.begin();
  while (lock_request_iter != request_queue->request_queue_.end()) {
    if ((*lock_request_iter)->rid_ == rid) {
      break;
    }
    lock_request_iter++;
  }
  if (lock_request_iter == request_queue->request_queue_.end()) {
    LOG_DEBUG("exception UnlockRow AbortReason::没找到要解锁的行 line:%d", __LINE__);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto lock_request = *lock_request_iter;

  UpdateTransactionState(txn, lock_request);
  DelTxnRowLockSet(txn, lock_request->oid_, lock_request->lock_mode_, rid);
  request_queue->request_queue_.erase(lock_request_iter);
  request_queue->cv_.notify_all();

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].emplace(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

// auto LockManager::Dfs(txn_id_t node, std::unordered_set<txn_id_t> &visited, std::unordered_set<txn_id_t> &rec_stack,
//                       txn_id_t *txn_id) -> bool {
//   if (rec_stack.find(node) != rec_stack.end()) {
//     *txn_id = *std::max_element(rec_stack.begin(), rec_stack.end());
//     return true;  // 发现环
//   }
//   if (visited.find(node) != visited.end()) {
//     return false;  // 已访问，但未发现环
//   }

//   visited.insert(node);
//   rec_stack.insert(node);
//   std::sort(waits_for_[node].begin(), waits_for_[node].end());
//   for (txn_id_t neighbor : waits_for_[node]) {
//     if (Dfs(neighbor, visited, rec_stack, txn_id)) {
//       return true;
//     }
//   }

//   rec_stack.erase(node);
//   return false;
// }

// auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
//   std::unordered_set<txn_id_t> visited;
//   std::unordered_set<txn_id_t> rec_stack;
//   std::vector<txn_id_t> wait_txns;
//   // 对key进行排序
//   for (auto &pair : waits_for_) {
//     wait_txns.emplace_back(pair.first);
//   }
//   std::sort(wait_txns.begin(), wait_txns.end());

//   for (auto txn : wait_txns) {
//     if (Dfs(txn, visited, rec_stack, txn_id)) {
//       return true;
//     }
//   }
//   return false;
// }

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // assume the graph is already fully built
  std::deque<txn_id_t> path;
  std::set<txn_id_t> visited;
  for (const auto &[start_node, end_node_set] : waits_for_) {
    if (visited.find(start_node) == visited.end()) {
      auto cycle_id = DepthFirstSearch(start_node, visited, path);
      if (cycle_id != NO_CYCLE) {
        // trim the path and retain only those involved in cycle
        auto it = std::find(path.begin(), path.end(), cycle_id);
        path.erase(path.begin(), it);
        std::sort(path.begin(), path.end());
        txn_id_t to_abort = path.back();
        *txn_id = to_abort;  // pick the youngest to abort
        return true;
      }
    }
  }
  return false;
}

auto LockManager::DepthFirstSearch(txn_id_t curr, std::set<txn_id_t> &visited, std::deque<txn_id_t> &path) -> txn_id_t {
  // mark curr node as visited and append to current path
  visited.insert(curr);
  path.push_back(curr);

  if (waits_for_.find(curr) != waits_for_.end()) {
    for (const auto &neighbor : waits_for_[curr]) {
      if (visited.find(neighbor) == visited.end()) {
        // this neighbor not visited yet
        auto cycle_id = DepthFirstSearch(neighbor, visited, path);
        if (cycle_id != NO_CYCLE) {
          // a cycle is detected ahead
          return cycle_id;
        }
      } else if (std::find(path.begin(), path.end(), neighbor) != path.end()) {
        // back edge detected
        return neighbor;
      }
    }
  }
  // remove from curr path
  path.pop_back();
  return NO_CYCLE;
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
    std::lock_guard<std::mutex> wait_for_lock(waits_for_latch_);

    table_lock_map_latch_.lock();
    std::unordered_map<txn_id_t, std::vector<std::shared_ptr<LockRequestQueue>>> txn_wait_ques;
    // 坑：这里没有对队列进行加锁
    for (const auto &[table_id, request_que] : table_lock_map_) {
      // 队列中没有等待事务
      std::lock_guard<std::mutex> que_lock(request_que->latch_);
      // if (request_que->request_queue_.empty() || request_que->request_queue_.back()->granted_) {
      //   continue;
      // }
      std::vector<txn_id_t> locked_txn;                            // 已经加锁的事务
      auto locked_end_iter = request_que->request_queue_.begin();  // 指向请求队列中等待加锁的事务
      while (locked_end_iter != request_que->request_queue_.end() && (*locked_end_iter)->granted_) {
        locked_txn.emplace_back((*locked_end_iter)->txn_id_);
        locked_end_iter++;
      }
      for (auto iter = locked_end_iter; iter != request_que->request_queue_.end(); iter++) {
        if (txn_wait_ques.find((*iter)->txn_id_) != txn_wait_ques.end()) {
          txn_wait_ques[(*iter)->txn_id_].emplace_back(request_que);
        } else {
          txn_wait_ques[(*iter)->txn_id_] = std::vector<std::shared_ptr<LockRequestQueue>>{request_que};
        }
        for (const auto &holder : locked_txn) {
          AddEdge((*iter)->txn_id_, holder);
        }
      }
    }
    table_lock_map_latch_.unlock();

    row_lock_map_latch_.lock();
    // 每一个等待事务，在等待哪一行
    for (const auto &[rid, request_que] : row_lock_map_) {
      std::lock_guard<std::mutex> que_lock(request_que->latch_);
      // 队列中没有等待事务
      // if (request_que->request_queue_.empty() || request_que->request_queue_.back()->granted_) {
      //   continue;
      // }
      std::vector<txn_id_t> locked_txn;
      auto locked_end_iter = request_que->request_queue_.begin();
      while (locked_end_iter != request_que->request_queue_.end() && (*locked_end_iter)->granted_) {
        locked_txn.emplace_back((*locked_end_iter)->txn_id_);
        locked_end_iter++;
      }
      for (auto iter = locked_end_iter; iter != request_que->request_queue_.end(); iter++) {
        if (txn_wait_ques.find((*iter)->txn_id_) != txn_wait_ques.end()) {
          txn_wait_ques[(*iter)->txn_id_].emplace_back(request_que);
        } else {
          txn_wait_ques[(*iter)->txn_id_] = std::vector<std::shared_ptr<LockRequestQueue>>{request_que};
        }

        for (const auto &holder : locked_txn) {
          AddEdge((*iter)->txn_id_, holder);
        }
      }
    }
    row_lock_map_latch_.unlock();
    txn_id_t youngest_txn = NO_CYCLE;
    // TODO(dsd) :当一个事务被Abort时是否需要找出该事务所有已经加锁、等待加锁的表/行？
    // 如果有环，将最年轻的事务Abort掉
    // 坑：这里应该采用while而不是if,要打破图中所有的环
    while (HasCycle(&youngest_txn)) {
      Transaction *txn = TransactionManager::GetTransaction(youngest_txn);
      txn->SetState(TransactionState::ABORTED);

      // 这里只移除了youngest_txn等待的事务
      // 没有移除等待youngest_txn的事务
      waits_for_.erase(youngest_txn);
      for (auto &[txn_id, wait_list] : waits_for_) {
        wait_list.erase(youngest_txn);
      }

      // 找到该事务所阻塞的表队列，将队列唤醒
      // auto table_iter = txn_wait_ques.find(youngest_txn);
      // if (table_iter != txn_wait_ques.end()) {
      //   for (auto &request_que : table_iter->second) {
      //     request_que->cv_.notify_all();
      //   }
      // }
    }
    if (youngest_txn != NO_CYCLE) {
      // if we ever find a single cycle to be aborted, notify everyone
      LockManager::NotifyAllTransaction();
    }

    waits_for_.clear();
  }
}

// void LockManager::RunCycleDetection() {
//   while (enable_cycle_detection_) {
//     std::this_thread::sleep_for(cycle_detection_interval);
//     {  // TODO(students): detect deadlock
//       // no more new transaction requests from this point
//       std::unique_lock table_lock(table_lock_map_latch_);
//       std::unique_lock row_lock(row_lock_map_latch_);
//       LockManager::RebuildWaitForGraph();
//       txn_id_t to_abort_txn = NO_CYCLE;
//       while (LockManager::HasCycle(&to_abort_txn)) {
//         // remove this transaction from graph
//         LockManager::TrimGraph(to_abort_txn);
//         // set this transaction as aborted
//         auto to_abort_ptr = TransactionManager::GetTransaction(to_abort_txn);
//         to_abort_ptr->SetState(TransactionState::ABORTED);
//       }
//       if (to_abort_txn != NO_CYCLE) {
//         // if we ever find a single cycle to be aborted, notify everyone
//         LockManager::NotifyAllTransaction();
//       }
//     }
//   }
// }

void LockManager::RebuildWaitForGraph() {
  waits_for_.clear();
  for (const auto &[table_id, request_queue] : table_lock_map_) {
    std::set<txn_id_t> granted;
    for (const auto &request : request_queue->request_queue_) {
      if (request->granted_) {
        granted.insert(request->txn_id_);
      } else {
        // waits for a resource, build an edge
        for (const auto &holder : granted) {
          AddEdge(request->txn_id_, holder);
        }
      }
    }
  }
  for (const auto &[row_id, request_queue] : row_lock_map_) {
    std::set<txn_id_t> granted;
    for (const auto &request : request_queue->request_queue_) {
      if (request->granted_) {
        granted.insert(request->txn_id_);
      } else {
        // waits for a resource, build an edge
        for (const auto &holder : granted) {
          AddEdge(request->txn_id_, holder);
        }
      }
    }
  }
}
void LockManager::TrimGraph(txn_id_t aborted_txn) {
  waits_for_.erase(aborted_txn);
  for (auto &[start_node, end_node_set] : waits_for_) {
    end_node_set.erase(aborted_txn);
  }
}

void LockManager::NotifyAllTransaction() {
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  for (const auto &[table_id, request_queue] : table_lock_map_) {
    request_queue->cv_.notify_all();
  }
  for (const auto &[row_id, request_queue] : row_lock_map_) {
    request_queue->cv_.notify_all();
  }
}

//////////////////////////////////////////////
// private helper functions
auto LockManager::IsLegalLockRequest(Transaction *txn, LockMode lock_mode) -> bool {
  // 0.检查事务的状态
  auto state = txn->GetState();
  auto txn_id = txn->GetTransactionId();
  auto isolation_level = txn->GetIsolationLevel();

  switch (isolation_level) {
    case IsolationLevel::REPEATABLE_READ:
      // 在 REPEATABLE_READ 隔离级别下，所有锁在生长状态下都允许，但在收缩状态下不允许任何锁
      if (state == TransactionState::SHRINKING) {
        LOG_DEBUG("exception IsLegalLockRequest AbortReason::LOCK_ON_SHRINKING line:%d", __LINE__);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // 在 READ_COMMITTED 隔离级别下，所有锁在生长状态下都允许，但在收缩状态下只允许 IS 和 S 锁
      if (state == TransactionState::SHRINKING &&
          !(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
        LOG_DEBUG("exception IsLegalLockRequest AbortReason::LOCK_ON_SHRINKING line:%d", __LINE__);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // 在 READ_UNCOMMITTED 隔离级别下，只允许 IX 和 X 锁，且不允许 S/IS/SIX 锁
      if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        LOG_DEBUG("exception IsLegalLockRequest AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED line:%d", __LINE__);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      // 坑：没有考虑收缩加锁的情况
      if (state == TransactionState::SHRINKING) {
        LOG_DEBUG("exception IsLegalLockRequest AbortReason::LOCK_ON_SHRINKING line:%d", __LINE__);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
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
  auto txn_id = txn->GetTransactionId();
  LockMode current_lock_mode = GetTableLockMode(txn, oid);
  // TODO(dsd) : 当一个事务有更高级别的锁时，是不是可以不用加锁了？
  // 当一个事务拥有更高级的锁，再去加低级的锁时，要抛出异常，也就是只能锁升级不能降级。为什么？
  // if (HasHigherLevelLock(current_lock_mode, lock_mode)) {
  //
  //   return true;
  // }
  if (current_lock_mode == lock_mode) {
    return true;
  }
  // Check for valid lock upgrade paths
  if (!CheckValidUpgrade(txn, current_lock_mode, lock_mode)) {
    throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
  }
  // Ensure only one transaction is upgrading its lock at a time
  if (IsTableBeingUpgrading(oid)) {
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
  }

  // Perform the lock upgrade
  return UpgradeLock(txn, oid, lock_mode);
}

auto LockManager::CheckValidUpgrade(Transaction *txn, LockMode current_lock_mode, LockMode lock_mode) -> bool {
  switch (current_lock_mode) {
    case LockMode::INTENTION_SHARED:
      break;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        return false;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_mode != LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      return false;
  }
  return true;
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

// auto LockManager::Compatible(const std::shared_ptr<LockRequestQueue> &lock_request_queue,
//                              const std::shared_ptr<LockRequest> &current_request) -> bool {
//   std::list<std::shared_ptr<LockRequest>> locked_request_queue;
//   std::list<std::shared_ptr<LockRequest>> unlocked_request_queue;
//   for (auto &lock_req : lock_request_queue->request_queue_) {
//     if (lock_req->granted_) {
//       locked_request_queue.emplace_back(lock_req);
//     } else {
//       unlocked_request_queue.emplace_back(lock_req);
//     }
//     if (lock_req == current_request) {
//       break;
//     }
//   }
//   // 1.判断当前锁请求与队列中所有已经加锁的请求是否兼容，如果不兼容返回false
//   if (!locked_request_queue.empty() && !Compatible(locked_request_queue, current_request)) {
//     return false;
//   }
//   // 2.有锁正在升级，如果升级请求为当前请求返回true，否则返回false
//   if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
//     return lock_request_queue->upgrading_ == current_request->txn_id_;
//   }
//   // for (auto &lock_req : unlocked_request_queue) {
//   //   if (!locked_request_queue.empty() && !Compatible(locked_request_queue, lock_req)) {
//   //     return false;
//   //   }
//   //   // 4.判断当前锁请求与前面waiting锁请求是否兼容
//   //   if (!Compatible(unlocked_request_queue, lock_req)) {
//   //     return false;
//   //   }
//   // }
//   // return true;
//   return std::all_of(unlocked_request_queue.begin(), unlocked_request_queue.end(),
//                      [this, &locked_request_queue, &unlocked_request_queue](const auto &lock_req) {
//                        return (locked_request_queue.empty() || Compatible(locked_request_queue, lock_req)) &&
//                               Compatible(unlocked_request_queue, lock_req);
//                      });
// }

auto LockManager::Compatible(const std::shared_ptr<LockRequestQueue> &lock_request_queue,
                             const std::shared_ptr<LockRequest> &lock_request) -> bool {
  for (auto &lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (lock_request.get() != lr.get()) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}

auto LockManager::Compatible(std::list<std::shared_ptr<LockRequest>> request_queue,
                             std::shared_ptr<LockRequest> current_request) -> bool {
  auto is_compatible = [&request_queue, &current_request](const std::function<bool(LockMode)> &checkCondition) {
    for (auto &request : request_queue) {
      if (request == current_request) {
        break;
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
      return request_queue.front() == current_request;
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
  while (prev_req != request_queue->request_queue_.end() && (*prev_req)->txn_id_ != txn->GetTransactionId()) {
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

  while (!Compatible(request_queue, lr) && txn->GetState() != TransactionState::ABORTED) {
    request_queue->cv_.wait(request_queue_lock);
  }
  // 事务在等待加锁的过程中被外界abort了
  // 由于此时还未获得锁，TransactionManager调用Abort函数也不会清理lr请求，所以需要手动清理并notify_all
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue->request_queue_.remove(lr);
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

  if (lock_mode != LockMode::EXCLUSIVE) {
    request_queue->cv_.notify_all();
  }
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
      if (lock_request->lock_mode_ == LockMode::EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED) {
        txn->SetState(TransactionState::SHRINKING);
      }
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
      break;
    case LockMode::SHARED:
      if (IsTableLocked(txn, oid)) {
        return true;
      }
      break;
    default:
      return false;
  }
  return false;
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
  // if (HasHigherLevelLock(current_lock_mode, lock_mode)) {
  //   return true;
  // }
  if (current_lock_mode == lock_mode) {
    return true;
  }
  // Check for valid lock upgrade paths
  if (!CheckValidUpgrade(txn, current_lock_mode, lock_mode)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  // Ensure only one transaction is upgrading its lock at a time
  if (IsRowBeingUpgrading(rid)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // Perform the lock upgrade
  return UpgradeLock(txn, oid, lock_mode, true, rid);
}

void TryUpgrade() {}

}  // namespace bustub
