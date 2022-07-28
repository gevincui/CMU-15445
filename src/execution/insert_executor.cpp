//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {
  // 递归初始化子执行器
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }

  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  // 根据子语句，选择不同的类型的插入执行方式
  Tuple to_insert_tuple;
  if (plan_->IsRawInsert()) {
    // 类型一：直接插入
    // 只有插入语句，如：INSERT INTO empty_table2 VALUES (100, 10), (101, 11), (102, 12)
    if (next_insert_ >= plan_->RawValues().size()) {
      // 如果要插的数据都插了，则退出循环
      return false;
    }
    // 如果要插入的数据还没插完，组合value值得到下一个要插的tuple
    to_insert_tuple = Tuple(plan_->RawValuesAt(next_insert_), &(table_info_->schema_));
    ++next_insert_;
  } else {
    // 类型二：间接插入
    // 从子语句获取要插入的值，如：INSERT INTO empty_table2 (SELECT col_a, col_b FROM test_1 WHERE col_a < 500)
    RID emit_rid;
    // 孩子节点找到第一个满足子语句的tuple，rid和tuple作为参数返回
    if (!child_executor_->Next(&to_insert_tuple, &emit_rid)) {
      return false;
    }
  }

  // 向表中插入tuple，同时把写操作记录到原表write set中
  bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());

  // 给新插入的rid加写锁
  if (inserted && !exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
    return false;
  }

  // 遍历该表所有索引，更新索引
  if (inserted) {
    std::for_each(table_indexes.begin(), table_indexes.end(),
        [&to_insert_tuple, &rid, &table_info = table_info_, &ctx = exec_ctx_](IndexInfo *index) {
              // 将插入的tuple中的索引字段值插入到对应的B+树中
              index->index_->InsertEntry(to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()), *rid, ctx->GetTransaction());
              // 把插入操作记录到索引的write set中
              ctx->GetTransaction()->GetIndexWriteSet()->emplace_back(*rid, table_info->oid_, WType::INSERT, to_insert_tuple, Tuple{}, index->index_oid_, ctx->GetCatalog());
    });
  }

  return inserted;
}

}  // namespace bustub
