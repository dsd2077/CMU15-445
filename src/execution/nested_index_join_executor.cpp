//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(child_executor)),
      inner_table_index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      inner_table_(exec_ctx_->GetCatalog()->GetTable(inner_table_index_info_->table_name_)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { left_child_->Init(); }

auto NestIndexJoinExecutor::ConstructJoinTuple(const Tuple &left_tuple, const Schema &left_schema,
                                               const Tuple &right_tuple, const Schema &right_schema) -> Tuple {
  std::vector<Value> values;
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple.GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    values.push_back(right_tuple.GetValue(&right_schema, i));
  }
  return {values, &plan_->OutputSchema()};
}

auto NestIndexJoinExecutor::ConstructNullJoinTuple(const Tuple &left_tuple, const Schema &left_schema,
                                                   const Schema &right_schema) -> Tuple {
  std::vector<Value> values;
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple.GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    auto value_type = right_schema.GetColumn(i).GetType();
    values.push_back(ValueFactory::GetNullValueByType(value_type));
  }
  return {values, &plan_->OutputSchema()};
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    Tuple left_tuple;
    if (!left_child_->Next(&left_tuple, rid)) {
      return false;
    }

    // 拿到外表的值去内表走索引
    Value key_value = plan_->key_predicate_->Evaluate(&left_tuple, left_child_->GetOutputSchema());
    Tuple key_tuple({key_value}, &inner_table_index_info_->key_schema_);
    std::vector<bustub::RID> result;
    inner_table_index_info_->index_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
    assert(result.size() <= 1);  // 测试用例保证不会往索引上插入相同的值

    if (result.size() == 1) {
      Tuple right_tuple;
      inner_table_->table_->GetTuple(result[0], &right_tuple, exec_ctx_->GetTransaction(), true);
      *tuple = ConstructJoinTuple(left_tuple, left_child_->GetOutputSchema(), right_tuple, inner_table_->schema_);
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      *tuple = ConstructNullJoinTuple(left_tuple, left_child_->GetOutputSchema(), inner_table_->schema_);
      return true;
    }
  }
}
}  // namespace bustub
