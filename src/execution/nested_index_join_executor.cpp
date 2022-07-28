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

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  inner_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info_->name_);
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_raw_tuple;

  // 拿到左表、右表满足JOIN条件(on条件)的下一个tuple
  do {
    // 左表拿到满足条件的下一个tuple(seq_scan方式)
    if (!child_executor_->Next(&left_tuple, &left_rid)) {
      return false;
    }
    // 右表拿到满足条件的下一个tuple(索引检索方式)
  } while (!Probe(&left_tuple, &right_raw_tuple) ||
           (plan_->Predicate() != nullptr &&
            !plan_->Predicate()
                 ->EvaluateJoin(&left_tuple, plan_->OuterTableSchema(), &right_raw_tuple, &(inner_table_info_->schema_))
                 .GetAs<bool>()));

  // 将拿到的满足JOIN条件(on条件)的左tuple和右tuple拼接起来作为参数返回
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values),
                 [&left_tuple = left_tuple, &right_raw_tuple, &plan = plan_,
                  &inner_table_schema = inner_table_info_->schema_](const Column &col) {
                   return col.GetExpr()->EvaluateJoin(&left_tuple, plan->OuterTableSchema(), &right_raw_tuple,
                                                      &inner_table_schema);
                 });

  *tuple = Tuple(values, plan_->OutputSchema());

  return true;
}

// 根据左表拿到tuple和JOIN条件，通过索引检索的方式去右表拿到对应tuple
bool NestIndexJoinExecutor::Probe(Tuple *left_tuple, Tuple *right_raw_tuple) {

  // 拿到右表的满足JOIN条件(on条件)的索引字段值
  Value key_value = plan_->Predicate()->GetChildAt(0)->EvaluateJoin(left_tuple, plan_->OuterTableSchema(),
                                                                    right_raw_tuple, &(inner_table_info_->schema_));
  Tuple probe_key = Tuple{{key_value}, inner_index_info_->index_->GetKeySchema()};

  // 根据索引字段值去B+树检索得到对应rid
  // 因为是唯一索引，所以结果集大小为1
  std::vector<RID> result_set;
  GetBPlusTreeIndex()->ScanKey(probe_key, &result_set, exec_ctx_->GetTransaction());
  if (result_set.empty()) {
    return false;
  }

  // 实现隔离级别
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      // 如果是读未提交隔离级别，不加读锁
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      // 如果是可重复读隔离级别，申请读锁，事务提交再释放
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(result_set[0]) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(result_set[0]) &&
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), result_set[0])) {
        // 如果该事务没拿到读锁或写锁，且申请读锁失败，则直接return
        return false;
      }
      break;
    default:
      break;
  }

  // 根据rid到原表拿到对应tuple
  bool searched = inner_table_info_->table_->GetTuple(result_set[0], right_raw_tuple, exec_ctx_->GetTransaction());

  // 如果是读提交隔离级别，用完锁即释放
  if (searched && exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    return exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), result_set[0]);
  }

  return searched;
}

}  // namespace bustub
