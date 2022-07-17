//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), table_iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  table_oid_t table_id = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  TableIterator end = table_info_->table_->End();
  auto predicate = plan_->GetPredicate();
  if (predicate != nullptr) {
    while (table_iter_ != end && !predicate->Evaluate(&(*table_iter_), &table_info_->schema_).GetAs<bool>()) {
      ++table_iter_;
    }
  }
  if (table_iter_ == end) {
    return false;
  }

  RID tuple_rid = table_iter_->GetRid();
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  auto isolation_level = txn->GetIsolationLevel();
  if (lock_manager != nullptr && isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    if (!lock_manager->LockShared(txn, tuple_rid)) {
      return false;
    }
  }
  *tuple = *table_iter_;
  // Use old RID because we haven't intialized page_id and slot_num for the new tuple
  *rid = tuple_rid;

  // Construct a new tuple (with column order possibly different)
  std::vector<Value> values;
  const Schema *schema = plan_->OutputSchema();
  uint32_t column_count = schema->GetColumnCount();
  for (uint32_t idx = 0; idx < column_count; ++idx) {
    values.push_back(schema->GetColumn(idx).GetExpr()->Evaluate(tuple, &table_info_->schema_));
  }
  *tuple = Tuple(values, schema);

  if (lock_manager != nullptr && isolation_level == IsolationLevel::READ_COMMITTED) {
    if (!lock_manager->Unlock(txn, tuple_rid)) {
      return false;
    }
  }

  ++table_iter_;
  return true;
}

}  // namespace bustub
