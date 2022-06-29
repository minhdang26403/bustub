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
    bool pull_succeed = child_executor_->Next(tuple, rid);
    if (!pull_succeed) {
        return false;
    }
    bool delete_succeed = table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (delete_succeed) {
        for (auto index_info : index_info_list_) {
            index_info->index_->DeleteEntry(tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), 
                    *rid, exec_ctx_->GetTransaction());
        }
    }
    return delete_succeed; 
}
}  // namespace bustub
