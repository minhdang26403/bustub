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
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  is_left_selected_ = left_executor_->Next(&left_tuple_, &left_rid_);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_id;
  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();
  while (is_left_selected_) {
    while (right_executor_->Next(&right_tuple, &right_id)) {
      auto is_satisfied = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
      if (is_satisfied.GetAs<bool>()) {
        // Generate new join tuple
        std::vector<Value> values;
        std::vector<Column> columns;
        uint32_t left_column_count = left_schema->GetColumnCount();
        for (uint32_t idx = 0; idx < left_column_count; ++idx) {
          values.push_back(left_tuple_.GetValue(left_schema, idx));
          columns.push_back(left_schema->GetColumn(idx));
        }
        uint32_t right_column_count = right_schema->GetColumnCount();
        for (uint32_t idx = 0; idx < right_column_count; ++idx) {
          values.push_back(right_tuple.GetValue(right_schema, idx));
          columns.push_back(right_schema->GetColumn(idx));
        }
        const Schema schema{columns};
        Tuple joined_tuple{values, &schema};
        *tuple = joined_tuple;
        *rid = joined_tuple.GetRid();
        return true;
      }
    }
    right_executor_->Init();
    is_left_selected_ = left_executor_->Next(&left_tuple_, &left_rid_);
  }
  return false;
}

}  // namespace bustub
