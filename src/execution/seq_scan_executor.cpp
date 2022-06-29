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
    while (table_iter_ != end && predicate->Evaluate(&(*table_iter_), &table_info_->schema_).GetAs<bool>() == false) {
      ++table_iter_;
    }
  }
  if (table_iter_ == end) {
    return false;
  }
  *tuple = *table_iter_;
  *rid = table_iter_->GetRid();
  ++table_iter_;
  return true;
}

}  // namespace bustub
