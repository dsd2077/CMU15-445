//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)) {}

void IndexScanExecutor::Init() {
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tree_->GetEndIterator()) {
    return false;
  }
  *rid = (*iter_).second;
  table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction(), true);
  ++iter_;
  return true;
}
}  // namespace bustub
