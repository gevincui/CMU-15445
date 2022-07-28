//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_{plan} {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void SeqScanExecutor::Init() {
  // 初始化表迭代器
  table_iter = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 迭代器找到满足比较条件(where条件作为Predicate存到plan中)的第一个tuple，退出循环
  Tuple raw_tuple;
  do {
    if (*table_iter == table_info_->table_->End()) {
      return false;
    }

    raw_tuple = *(*table_iter);

    ++(*table_iter);
  } while (plan_->GetPredicate() != nullptr &&
           !plan_->GetPredicate()->Evaluate(&raw_tuple, &(table_info_->schema_)).GetAs<bool>());

  // 实现隔离级别
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      // 如果是读未提交隔离级别，不加读锁
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      // 如果是可重复读隔离级别，申请读锁，事务提交再释放
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(raw_tuple.GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(raw_tuple.GetRid()) &&
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), raw_tuple.GetRid())) {
        // 如果该事务没拿到读锁或写锁，且申请读锁失败，则直接return
        return false;
      }
      break;
    default:
      break;
  }

  // 将tuple的每个列字段值存入到values
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values), [&raw_tuple, &table_info = table_info_](const Column &col) {
                   return col.GetExpr()->Evaluate(&raw_tuple, &(table_info->schema_));
                 });

  // 将tuple和rid作为参数返回
  *tuple = Tuple{values, plan_->OutputSchema()};
  *rid = raw_tuple.GetRid();

  // 如果是读提交隔离级别，用完锁即释放
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    return exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), raw_tuple.GetRid());
  }

  return true;
}

}  // namespace bustub
