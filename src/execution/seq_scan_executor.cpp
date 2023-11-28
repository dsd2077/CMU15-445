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
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      bool is_locked = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!is_locked) {
        LOG_DEBUG("SeqScan Executor Get Table Lock Failed line:%d", __LINE__);
        throw std::runtime_error("SeqScan Executor Get Table Lock Failed");
      }
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
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                            table_info_->oid_, table_iter_->GetRid());
      if (!is_locked) {
        LOG_DEBUG("SeqScan Executor Get Row Lock Failed line:%d", __LINE__);
        throw std::runtime_error("SeqScan Executor Get Row Lock Failed");
      }
    }
  } catch (const TransactionAbortException &) {
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw;
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == table_info_->table_->End()) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      const auto locked_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(table_info_->oid_);
      table_oid_t oid = table_info_->oid_;
      for (auto rid : locked_row_set) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid);
      }

      exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
    }
    return false;
  }

  *tuple = *table_iter_;
  *rid = table_iter_->GetRid();
  table_iter_++;
  // 读之前加锁
  try {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        table_iter_ != table_info_->table_->End()) {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                            table_info_->oid_, table_iter_->GetRid());
      if (!is_locked) {
        LOG_DEBUG("SeqScan Executor Get Row Lock Failed line:%d", __LINE__);
        throw std::runtime_error("SeqScan Executor Get Row Lock Failed");
      }
    }
  } catch (const TransactionAbortException &) {
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw;
  }
  return true;
}

}  // namespace bustub
