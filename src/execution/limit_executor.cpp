//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {}

void LimitExecutor::Init() {
  child_executor_->Init();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {

  // 从第m到m+n条中，找到满足条件的下一条tuple和rid，作为参数返回
  do {
    if (!child_executor_->Next(tuple, rid) || emitted >= plan_->GetLimit()) {
      return false;
    }
  } while (skipped++ < plan_->GetOffset());

  ++emitted;

  return true;
}


}  // namespace bustub
