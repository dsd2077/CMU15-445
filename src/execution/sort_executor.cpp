#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // 加一个第一次的判断
  child_executor_->Init();
  all_data_.clear();
  FetchAndSortTable();
  iter_ = all_data_.begin();
}

void SortExecutor::FetchAndSortTable() {
  // 拿到表的所有数据
  all_data_.emplace_back(Tuple());
  RID child_rid;
  while (child_executor_->Next(&all_data_.back(), &child_rid)) {
    all_data_.emplace_back(Tuple());
  }
  all_data_.pop_back();  // 扔掉最后一个空Tuple

  auto cmp = [&](const Tuple &a, const Tuple &b) {
    const std::vector<std::pair<bustub::OrderByType, bustub::AbstractExpressionRef>> &orderbys = plan_->GetOrderBy();
    for (auto &orderby_unit : orderbys) {
      Value left = orderby_unit.second->Evaluate(&a, child_executor_->GetOutputSchema());
      Value right = orderby_unit.second->Evaluate(&b, child_executor_->GetOutputSchema());
      if (left.CompareEquals(right) == CmpBool::CmpTrue) {
        continue;  // 相等比较下一个字段
      }
      switch (orderby_unit.first) {
        case OrderByType::ASC:
        case OrderByType::DEFAULT:
          return left.CompareLessThan(right) == CmpBool::CmpTrue;
          break;
        case OrderByType::DESC:
          return left.CompareGreaterThan(right) == CmpBool::CmpTrue;
        default:
          break;
      }
    }
    return false;  // 完全相等
  };

  std::sort(all_data_.begin(), all_data_.end(), cmp);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == all_data_.end()) {
    return false;
  }
  *tuple = *iter_;
  ++iter_;
  return true;
}

}  // namespace bustub
