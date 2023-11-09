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

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (all_done) return false;
  // 将所有元素插入，然后返回false
  Tuple child_tuple{};
  int count = 0;
  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);
  while (child_executor_->Next(&child_tuple, rid)) {
    table->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    count++;
    for (auto index_info : indexs) {
      // 提取出索引字段
      Tuple key_tuple = child_tuple.KeyFromTuple(table->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }

  *tuple = Tuple{{Value(TypeId::INTEGER, count)}, &GetOutputSchema()};
  all_done = true;
  return true; 
}
}  // namespace bustub
