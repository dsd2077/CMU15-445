#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  top_n_.clear();

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

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> heap(cmp);

  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    if (heap.size() < plan_->GetN()) {
      heap.push(child_tuple);
    } else if (cmp(child_tuple, heap.top())) {
      heap.pop();
      heap.push(child_tuple);
    }
  }
  while (!heap.empty()) {
    top_n_.push_back(heap.top());
    heap.pop();
  }
  std::reverse(top_n_.begin(), top_n_.end());
  iter_ = top_n_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == top_n_.end()) {
    return false;
  }
  *tuple = *iter_;
  iter_++;
  return true;
}
}  // namespace bustub
