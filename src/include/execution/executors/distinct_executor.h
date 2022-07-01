//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

/** Key represents a key generated from tuple attributes to discard duplicate values */
struct Key {
  /** The tuple attributes */
  std::vector<Value> attrs_;

  /**
   * Compares two keys for equality.
   * @param other the other key to be compared with
   * @return `true` if both keys have equivalent expressions, `false` otherwise
   */
  bool operator==(const Key &other) const {
    for (uint32_t i = 0; i < other.attrs_.size(); i++) {
      if (attrs_[i].CompareEquals(other.attrs_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::Key> {
  std::size_t operator()(const bustub::Key &agg_key) const {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.attrs_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  /**
   * @Compare two tuples on all attributes
   * @param lhs the left tuple
   * @param rhs the right tuple
   * @return `true` if tuples are the same, `false` otherwise
   */
  bool CompareTuple(const Tuple &lhs, const Tuple &rhs);

 private:
  /** @return The tuple as an Key */
  Key MakeKeyFromAttr(const Tuple *tuple) {
    std::vector<Value> attrs;
    const Schema *schema = child_executor_->GetOutputSchema();
    std::size_t column_count = schema->GetColumnCount();
    for (uint32_t idx = 0; idx < column_count; ++idx) {
      attrs.push_back(tuple->GetValue(schema, idx));
    }
    return {attrs};
  }
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The hash table is just a map from returned attributes to a set of tuples */
  std::unordered_map<Key, std::vector<Tuple>> ht_{};
  /** The list of tuples to be produced by Next */
  std::vector<Tuple> result_;
  /** The number of tuples produced */
  std::size_t count_{0};
};
}  // namespace bustub
