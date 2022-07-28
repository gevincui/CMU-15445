//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_{std::move(child)},
      aht_{plan_->GetAggregates(), plan_->GetAggregateTypes()},
      aht_iterator_{aht_.Begin()} {}

// 聚合操作在本执行器的Init方法已计算完成
void AggregationExecutor::Init() {
  child_->Init();

  Tuple tuple;
  RID rid;
  // 子节点执行器用于查找满足条件(where条件，seq_scan)的下一个tuple
  while (child_->Next(&tuple, &rid)) {

    // 哈希表维护的key是group by的字段，对应value是这个group by字段的聚合操作的结果
    // 如果不存在group by 条件，则哈希表只有一个key为{}
    // 如果存在group by 字段a，则哈希表的key为字段a的所有不同值
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }

  // 聚合操作已完成，把迭代器指针指向哈希表第一个key
  aht_iterator_ = aht_.Begin();
}

// 查找满足条件的下一个tuple(having条件)
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {

  // group by字段值
  std::vector<Value> group_bys;
  // 聚合结果字段值
  std::vector<Value> aggregates;

  // 取符合having条件的下一条
  do {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }

    group_bys = aht_iterator_.Key().group_bys_;
    aggregates = aht_iterator_.Val().aggregates_;

    ++aht_iterator_;
  } while (plan_->GetHaving() != nullptr &&
           !plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>());

  // 将group by字段值和聚合结果字段值拼接成tuple
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values), [&group_bys, &aggregates](const Column &col) {
                   return col.GetExpr()->EvaluateAggregate(group_bys, aggregates);
                 });

  // 将tuple作为参数返回
  *tuple = Tuple{values, plan_->OutputSchema()};

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
