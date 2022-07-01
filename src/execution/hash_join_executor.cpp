//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  right_child_->Init();
  left_child_->Init();
  Tuple tuple;
  RID rid;
  while (left_child_->Next(&tuple, &rid)) {
    Value left_join_attr = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_child_->GetOutputSchema());
    JoinKey left_join_key{left_join_attr};
    if (ht_.count(left_join_key) > 0) {
      ht_[left_join_key].push_back(tuple);
    } else {
      ht_[left_join_key] = {tuple};
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // Outer join table is empty
  if (ht_.empty()) {
    return false;
  }
  if (buffer_idx_ < duplicate_buffer_.size()) {
    *tuple = duplicate_buffer_[buffer_idx_++];
    return true;
  }
  Tuple right_tuple;
  if (!right_child_->Next(&right_tuple, rid)) {
    return false;
  }
  Value right_join_attr = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_->GetOutputSchema());
  JoinKey right_join_key{right_join_attr};

  while (ht_[right_join_key].empty()) {
    if (!right_child_->Next(&right_tuple, rid)) {
      return false;
    }
    right_join_key.join_attr_ =
        plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_->GetOutputSchema());
  }

  // Stupid bug
  // right_join_attr = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_->GetOutputSchema());

  // Each join_attr uniquely identifies a JoinKey object (no need to check for multiple join_attrs map
  // the same join key)
  for (const auto &left_tuple : ht_[right_join_key]) {
    *tuple =
        GenerateJoinTuple(&left_tuple, left_child_->GetOutputSchema(), &right_tuple, right_child_->GetOutputSchema());
    *rid = tuple->GetRid();
    duplicate_buffer_.push_back(*tuple);
  }
  // Handle the case where tuples in the outer table has duplicate join key
  // by buffering them to return later
  *tuple = duplicate_buffer_[buffer_idx_++];
  return true;
}

Tuple HashJoinExecutor::GenerateJoinTuple(const Tuple *left_tuple, const Schema *left_schema, const Tuple *right_tuple,
                                          const Schema *right_schema) {
  std::vector<Value> values;
  std::vector<Column> columns;
  uint32_t left_column_count = left_schema->GetColumnCount();
  for (uint32_t idx = 0; idx < left_column_count; ++idx) {
    values.push_back(left_tuple->GetValue(left_schema, idx));
    columns.push_back(left_schema->GetColumn(idx));
  }
  uint32_t right_column_count = right_schema->GetColumnCount();
  for (uint32_t idx = 0; idx < right_column_count; ++idx) {
    values.push_back(right_tuple->GetValue(right_schema, idx));
    columns.push_back(right_schema->GetColumn(idx));
  }
  const Schema schema{columns};
  Tuple joined_tuple{values, &schema};
  return joined_tuple;
}

}  // namespace bustub
