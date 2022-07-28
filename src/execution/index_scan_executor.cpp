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
    : AbstractExecutor(exec_ctx), plan_{plan} {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
}

void IndexScanExecutor::Init() {
  // 初始化索引迭代器
  index_iter_ = std::make_unique<INDEXITERATOR_TYPE>(GetBPlusTreeIndex()->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 迭代器找到满足比较条件(where条件作为Predicate存到plan中)的下一个tuple，退出循环
  Tuple raw_tuple;
  do {
    if (*index_iter_ == GetBPlusTreeIndex()->GetEndIterator()) {
      return false;
    }
    bool fetched = table_info_->table_->GetTuple((*(*index_iter_)).second, &raw_tuple, exec_ctx_->GetTransaction());
    if (!fetched) {
      return false;
    }
    ++(*index_iter_);
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
