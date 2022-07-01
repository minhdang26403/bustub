//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    Key key = MakeKeyFromAttr(&tuple);
    if (ht_.count(key) == 0) {
      ht_[key] = {tuple};
    } else {
      bool same = false;
      for (const auto &existing_tuple : ht_[key]) {
        if (CompareTuple(existing_tuple, tuple)) {
          same = true;
          break;
        }
      }
      if (!same) {
        ht_[key].push_back(tuple);
      }
    }
  }
  for (const auto &iter : ht_) {
    for (const auto &tuple : iter.second) {
      result_.push_back(tuple);
    }
  }
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (count_ == result_.size()) {
    return false;
  }
  *tuple = result_[count_++];
  *rid = tuple->GetRid();
  return true;
}

bool DistinctExecutor::CompareTuple(const Tuple &lhs, const Tuple &rhs) {
  const Schema *schema = child_executor_->GetOutputSchema();
  uint32_t column_count = schema->GetColumnCount();
  for (uint32_t idx = 0; idx < column_count; ++idx) {
    if (lhs.GetValue(schema, idx).CompareNotEquals(rhs.GetValue(schema, idx)) == CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

}  // namespace bustub
