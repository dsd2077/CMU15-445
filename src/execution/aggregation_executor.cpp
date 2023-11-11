//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_{aht_.Begin()} {}

void AggregationExecutor::Init() {
  child_->Init();
  aht_.Clear();
  is_empty_res_ = true;

  Tuple child_tuple{};
  RID child_rid{};
  while (child_->Next(&child_tuple, &child_rid)) {
    is_empty_res_ = false;
    AggregateKey agg_key = MakeAggregateKey(&child_tuple);
    AggregateValue agg_value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key, agg_value);
  }
  // 当结果为空，并且全部是聚集函数时
  if (is_empty_res_ && plan_->GetGroupBys().empty()) {
    aht_.InsertCombine({}, aht_.GenerateInitialAggregateValue());
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  auto output = aht_iterator_.Key().group_bys_;
  auto aggregates = aht_iterator_.Val().aggregates_;
  output.insert(output.end(), aggregates.begin(), aggregates.end());
  *tuple = Tuple{output, &plan_->OutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
