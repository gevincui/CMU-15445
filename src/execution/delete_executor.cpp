//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void DeleteExecutor::Init() {
  // 递归初始化子执行器
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }

  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  // 孩子节点找到第一个满足子语句(index_scan或seq_scan)的tuple，即要进行删除的第一条tuple，rid和tuple作为参数返回
  Tuple to_delete_tuple;
  RID emit_rid;
  if (!child_executor_->Next(&to_delete_tuple, &emit_rid)) {
    return false;
  }

  // 标记删除原表的tuple
  bool marked = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());

  // 遍历该表所有索引，更新索引
  if (marked) {
    std::for_each(table_indexes.begin(), table_indexes.end(),
                  [&to_delete_tuple, &emit_rid, &table_info = table_info_, &ctx = exec_ctx_](IndexInfo *index) {
                    // 将删除的tuple中的索引字段值在对应的B+树也删除
                    index->index_->DeleteEntry(to_delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                            index->index_->GetKeyAttrs()),
                                               emit_rid, ctx->GetTransaction());
                  });
  }

  return marked;
}

}  // namespace bustub
