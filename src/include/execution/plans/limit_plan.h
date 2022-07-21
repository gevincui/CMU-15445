//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_plan.h
//
// Identification: src/include/execution/plans/limit_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * Limit constraints the number of output tuples produced by its child executor.
 */
class LimitPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new limit plan node that has a child plan.
   * @param child the child plan to obtain tuple from
   * @param limit the number of output tuples
   * @param offset the number of rows to be skipped
   */
  LimitPlanNode(const Schema *output_schema, const AbstractPlanNode *child, size_t limit, size_t offset)
      : AbstractPlanNode(output_schema, {child}), limit_(limit), offset_(offset) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Limit; }

  /** @return The limit */
  auto GetLimit() const -> size_t { return limit_; }

  /** @return The offset */
  auto GetOffset() const { return offset_; }

  /** @return The child plan node */
  auto GetChildPlan() const -> const AbstractPlanNode * {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Limit should have at most one child plan.");
    return GetChildAt(0);
  }

 private:
  /** The limit */
  std::size_t limit_;
  /** The offset */
  size_t offset_;
};

}  // namespace bustub
