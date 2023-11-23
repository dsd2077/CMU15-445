//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <iterator>  // 包含 prev 函数
#include "concurrency/transaction_manager.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)),
      table_iter_(table_info_->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
  try {
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      // REPEATABLE_READ实施严格二阶段锁，GROWING阶段不允许解锁，如果加IS锁的话，需要加大量的行锁，不如直接加S锁
      // case IsolationLevel::REPEATABLE_READ:
      //   exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
      //                                          table_info_->oid_);
      //   break;
      // READ_COMMITED允许在GROWING阶段解S锁，读一行解一个行锁
      // 正是因为在GROWING阶段将S锁解了，其他事务才能对该数据进行修改操作。导致不可重复读
      case IsolationLevel::REPEATABLE_READ:
      case IsolationLevel::READ_COMMITTED:
        exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                               table_info_->oid_);
        break;
      // READ_UNCOMMITED不加读锁
      // 因为不加S锁，所以可以读到其他事务未提交的数据
      case IsolationLevel::READ_UNCOMMITTED:
        break;
    }
  } catch (const TransactionAbortException &) {
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw;
  }

  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  // 读之前加锁
  try {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        table_iter_ != table_info_->table_->End()) {
      exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                           table_info_->oid_, table_iter_->GetRid());
    }
  } catch (const TransactionAbortException &) {
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw;
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == table_info_->table_->End()) {
    return false;
  }

  *tuple = *table_iter_;
  *rid = table_iter_->GetRid();
  // 读之后解锁
  try {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, table_iter_->GetRid());
    }
  } catch (const TransactionAbortException &) {
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw;
  }

  table_iter_++;
  // 读之前加锁
  try {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        table_iter_ != table_info_->table_->End()) {
      exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                           table_info_->oid_, table_iter_->GetRid());
    }
  } catch (const TransactionAbortException &) {
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw;
  }
  return true;
}

}  // namespace bustub
