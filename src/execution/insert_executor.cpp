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

  bool insert_succeed = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
  if (insert_succeed) {
    for (auto index_info : index_info_list_) {
      index_info->index_->InsertEntry(
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
          exec_ctx_->GetTransaction());
    }
  }

  return insert_succeed;
}

}  // namespace bustub
