//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_list_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    if (idx_ >= plan_->RawValues().size()) {
      return false;
    }
    *tuple = Tuple(plan_->RawValuesAt(idx_), &table_info_->schema_);
    ++idx_;
  } else {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
  }

  if (!table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  RID insert_tuple_rid = *rid;
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  if (lock_manager != nullptr) {
    if (!lock_manager->LockExclusive(txn, insert_tuple_rid)) {
      return false;
    }
  }

  for (auto index_info : index_info_list_) {
    index_info->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
        insert_tuple_rid, exec_ctx_->GetTransaction());
    IndexWriteRecord index_write_record{insert_tuple_rid, table_info_->oid_,      WType::INSERT,          *tuple,
                                        *tuple,           index_info->index_oid_, exec_ctx_->GetCatalog()};
    txn->GetIndexWriteSet()->emplace_back(index_write_record);
  }
  return true;
}

}  // namespace bustub
