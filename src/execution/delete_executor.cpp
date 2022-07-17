//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_list_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  RID delete_tuple_rid = *rid;
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  if (lock_manager != nullptr) {
    if (!lock_manager->LockExclusive(txn, delete_tuple_rid)) {
      return false;
    }
  }

  if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
    return false;
  }

  for (auto index_info : index_info_list_) {
    index_info->index_->DeleteEntry(
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());
    IndexWriteRecord index_write_record{delete_tuple_rid, table_info_->oid_,      WType::DELETE,          *tuple,
                                        *tuple,           index_info->index_oid_, exec_ctx_->GetCatalog()};
    txn->GetIndexWriteSet()->emplace_back(index_write_record);
  }
  return true;
}

}  // namespace bustub
