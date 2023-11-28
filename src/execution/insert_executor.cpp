//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // TDOO(dsd) :在火山模型中，是否存在对同一张表的反复加锁？是否存在锁冲突？
  // 不会的，如果是同一事务对同一张表进行加锁，如果锁的级别相同会直接返回，如果锁的级别不同会试图进行锁升级
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      LOG_DEBUG("Insert Executor Get Table Lock Failed line:%d", __LINE__);
      throw std::runtime_error("Insert Executor Get Table Lock Failed");
    }
  } catch (const TransactionAbortException &) {
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw;
  }
  all_done_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (all_done_) {
    return false;
  }
  // 将所有元素插入，然后返回false
  Tuple child_tuple{};
  int count = 0;
  std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&child_tuple, rid)) {
    // 插入数据时是否需要加写锁？——不需要，不是不需要而是加不了，因为插入之前RID都不确定,如果能加锁的话根本就不会出现幻读,
    table_info_->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    // TODO(dsd) 是否有必要对rid加行锁？——有必要，以防止在当前事务提交之前，其他事务读取或修改这个新插入的行。
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                            LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
      if (!is_locked) {
        LOG_DEBUG("Insert Executor Get Table Lock Failed line:%d", __LINE__);
        throw std::runtime_error("Insert Executor Get Table Lock Failed");
      }
    } catch (const TransactionAbortException &) {
      exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
      throw;
    }

    // 坑：InsertTuple中已经将write_record插入了
    // TableWriteRecord write_record(*rid, WType::INSERT, child_tuple, table_info_->table_.get());
    // exec_ctx_->GetTransaction()->AppendTableWriteRecord(write_record);
    count++;
    for (auto index_info : indexs) {
      // 提取出索引字段
      Tuple key_tuple =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
      IndexWriteRecord index_record(*rid, table_info_->oid_, WType::INSERT, child_tuple, index_info->index_oid_,
                                    exec_ctx_->GetCatalog());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_record);
    }
  }

  *tuple = Tuple{{Value(TypeId::INTEGER, count)}, &GetOutputSchema()};
  all_done_ = true;
  return true;
}
}  // namespace bustub
