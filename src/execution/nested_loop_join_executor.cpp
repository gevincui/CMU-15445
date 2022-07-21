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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_{std::move(left_executor)},
      right_executor_{std::move(right_executor)} {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid;
  // 左表没有数据时，直接返回false
  // 拿到左表满足条件(where条件)的下一个tuple
  if (left_tuple.GetLength() == 0 && !left_executor_->Next(&left_tuple, &left_rid)) {
    return false;
  }

  // 拿到右表满足JOIN条件(on条件)的下一个tuple
  Tuple right_tuple;
  RID right_rid;
  do {
    if (!Advance(&left_rid, &right_tuple, &right_rid)) {
      return false;
    }
  } while (plan_->Predicate() != nullptr && !plan_->Predicate()
                                                 ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                                &right_tuple, right_executor_->GetOutputSchema())
                                                 .GetAs<bool>());

  // 将拿到的满足JOIN条件(on条件)的左tuple和右tuple拼接起来作为参数返回
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values),
                 [&left_tuple = left_tuple, &left_executor = left_executor_, &right_tuple,
                  &right_executor = right_executor_](const Column &col) {
                   return col.GetExpr()->EvaluateJoin(&left_tuple, left_executor->GetOutputSchema(), &right_tuple,
                                                      right_executor->GetOutputSchema());
                 });

  *tuple = Tuple(values, plan_->OutputSchema());

  return true;
}

bool NestedLoopJoinExecutor::Advance(RID *left_rid, Tuple *right_tuple, RID *right_rid) {
  // 拿到右表取不到满足条件(Where)的下一个tuple
  if (!right_executor_->Next(right_tuple, right_rid)) {
    // 如果左表也取不到满足条件(Where)的下一个tuple，返回false
    if (!left_executor_->Next(&left_tuple, left_rid)) {
      return false;
    }
    // 左表取到下一个tuple，右表没取到，所以右表从表头重新取下一个tuple
    right_executor_->Init();
    right_executor_->Next(right_tuple, right_rid);
  }

  return true;
}

}  // namespace bustub
