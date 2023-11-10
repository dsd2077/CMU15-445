//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_executor)),
      right_child_(std::move(right_executor)),
      join_predicate_(plan_->Predicate()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
 left_child_->Init();
 left_tuple_ = Tuple();
 right_tuple_ = Tuple();
 left_step_ = true;
 is_left_matched_ = false;
 right_step = true;
}

auto NestedLoopJoinExecutor::ConstructJoinTuple() -> Tuple {
  std::vector<Value> values;
  for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(right_tuple_.GetValue(&right_child_->GetOutputSchema(), i));
  }
  return {values, &plan_->OutputSchema()};
}

auto NestedLoopJoinExecutor::ConstructNullJoinTuple() -> Tuple {
  std::vector<Value> values;
  for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
    auto value_type = right_child_->GetOutputSchema().GetColumn(i).GetType();
    values.push_back(ValueFactory::GetNullValueByType(value_type));
  }
  return {values, &plan_->OutputSchema()};
}


auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while (1) {
    // 外表取下一行数据
    if (left_step_) {
      left_step_ = false;
      if (!left_child_->Next(&left_tuple_, rid)) return false;  // 外表遍历完
      right_child_->Init();
    }

    while ((right_step = right_child_->Next(&right_tuple_, rid))) {
      Value value = join_predicate_.EvaluateJoin(&left_tuple_, left_child_->GetOutputSchema(), &right_tuple_,
                                                 right_child_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        is_left_matched_ = true;
        *tuple = ConstructJoinTuple();
        return true;
      } 
    }
    if (!right_step) {
      left_step_ = true;
      // 左外连接：如果外表的某一行没有匹配到任何一行内表数据，需要补充一行空值数据
      if (plan_->GetJoinType() == JoinType::LEFT && !is_left_matched_) {
        *tuple = ConstructNullJoinTuple();
        is_left_matched_ = false;
        return true;
      }

      is_left_matched_ = false;
    }
  }
}

}  // namespace bustub
