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
      plan_(plan),
      child_(std::move(child)),
      aht_({plan->GetAggregates(), plan->GetAggregateTypes()}),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }

  // Reset the iterator after inserting new tuples because
  // successful insertion makes iterator invalid
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  SimpleAggregationHashTable::Iterator end = aht_.End();
  auto having_clause = plan_->GetHaving();
  if (having_clause != nullptr) {
    while (aht_iterator_ != end &&
           !having_clause->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
                .GetAs<bool>()) {
      ++aht_iterator_;
    }
  }

  if (aht_iterator_ == end) {
    return false;
  }

  std::vector<Value> values;
  const Schema *schema = GetOutputSchema();
  uint32_t col_count = schema->GetColumnCount();
  for (uint32_t idx = 0; idx < col_count; ++idx) {
    values.push_back(schema->GetColumn(idx).GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_,
                                                                         aht_iterator_.Val().aggregates_));
  }
  *tuple = Tuple{values, schema};
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
