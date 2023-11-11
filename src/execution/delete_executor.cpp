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

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (all_done_) {
    return false;
  }
  // 将所有元素插入，然后返回false
  Tuple child_tuple{};
  int count = 0;
  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);

  while (child_executor_->Next(&child_tuple, rid)) {
    table->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    count++;
    for (auto index_info : indexs) {
      Tuple key_tuple =
          child_tuple.KeyFromTuple(table->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }

  *tuple = Tuple{{Value(TypeId::INTEGER, count)}, &GetOutputSchema()};
  all_done_ = true;
  return true;
}

}  // namespace bustub
