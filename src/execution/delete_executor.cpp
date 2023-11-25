//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Delete Executor Get Table Lock Failed");
    }
  } catch (const TransactionAbortException &) {
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw;
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (all_done_) {
    return false;
  }
  // 将所有元素插入，然后返回false
  Tuple child_tuple{};
  int count = 0;
  std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  while (child_executor_->Next(&child_tuple, rid)) {
    table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                            LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("Delete Executor Get Row Lock Failed");
      }
    } catch (const TransactionAbortException &) {
      exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
      throw;
    }
    // TableWriteRecord write_record(*rid, WType::DELETE, child_tuple, table_info_->table_.get());
    // exec_ctx_->GetTransaction()->AppendTableWriteRecord(write_record);
    count++;
    for (auto index_info : indexs) {
      Tuple key_tuple =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
      IndexWriteRecord index_record(*rid, table_info_->oid_, WType::DELETE, child_tuple, index_info->index_oid_,
                                    exec_ctx_->GetCatalog());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_record);
    }
  }

  *tuple = Tuple{{Value(TypeId::INTEGER, count)}, &GetOutputSchema()};
  all_done_ = true;
  return true;
}

}  // namespace bustub
