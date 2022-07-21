//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() {
  // 递归初始化子执行器
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }

  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  // 孩子节点找到第一个满足子语句(index_scan或seq_scan)的tuple，即要进行更新的第一条tuple，rid和tuple作为参数返回
  Tuple dummy_tuple;
  RID emit_rid;
  if (!child_executor_->Next(&dummy_tuple, &emit_rid)) {
    return false;
  }

  // 通过rid拿到原表对应的tuple
  Tuple to_update_tuple;
  auto fetched = table_info_->table_->GetTuple(emit_rid, &to_update_tuple, exec_ctx_->GetTransaction());
  if (!fetched) {
    return false;
  }

  // 拿到更新后的tuple的数据
  Tuple updated_tuple = GenerateUpdatedTuple(to_update_tuple);

  // 更新原表tuple数据
  bool updated = table_info_->table_->UpdateTuple(updated_tuple, emit_rid, exec_ctx_->GetTransaction());

  // 遍历该表所有索引，更新索引
  if (updated) {
    std::for_each(
        table_indexes.begin(), table_indexes.end(),
        [&to_update_tuple, &updated_tuple, &emit_rid, &table_info = table_info_, &ctx = exec_ctx_](IndexInfo *index) {
          // 将旧的tuple中的索引字段值从对应的B+树中删除
          index->index_->DeleteEntry(
              to_update_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
              emit_rid, ctx->GetTransaction());
          // 将更新后的tuple中的索引字段从对应的B+树中插入
          index->index_->InsertEntry(
              updated_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
              emit_rid, ctx->GetTransaction());
          ctx->GetTransaction()->GetIndexWriteSet()->emplace_back(emit_rid, table_info->oid_, WType::UPDATE,
                                                                  updated_tuple, to_update_tuple, index->index_oid_,
                                                                  ctx->GetCatalog());
        });
  }

  return updated;
}


auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {

  // 记录哪些列需要更新，以及更新的值
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  // values存储整行tuple每一列的vlaue
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    // 如果当前列不需要更新，直接将旧值存到values
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      // 如果当前列需要更新，将更新值存到values
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:   // 如果更新操作是加法，将原值做加法后存到values
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:   // 如果更新操作是设为制定值，将原值更新后存到values
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
