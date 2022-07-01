//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** JoinKey represents a key in a join operation */
struct JoinKey {
  /** The join attribute */
  Value join_attr_;

  /**
   * Compares two join keys for equality.
   * @param other the other join key to be compared with
   * @return `true` if both join keys have equivalent join expressions (conditions), `false` otherwise
   */
  bool operator==(const JoinKey &other) const { return join_attr_.CompareEquals(other.join_attr_) == CmpBool::CmpTrue; }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::JoinKey> {
  std::size_t operator()(const bustub::JoinKey &join_key) const {
    size_t hash_value = 0;
    const auto &key = join_key.join_attr_;
    if (!key.IsNull()) {
      hash_value = bustub::HashUtil::CombineHashes(hash_value, bustub::HashUtil::HashValue(&key));
    }
    return hash_value;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  /**
   * Join the left and right tuple
   * @param left_tuple The left tuple
   * @param left_schema The schema of the left tuple
   * @param right_schema The schema of the right tuple
   * @param right_tuple The right tuple
   * @return The joined tuple
   */
  Tuple GenerateJoinTuple(const Tuple *left_tuple, const Schema *left_schema, const Tuple *right_tuple,
                          const Schema *right_schema);

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The left child executor to obtain value from */
  std::unique_ptr<AbstractExecutor> left_child_;
  /** The right child executor to obtain value from */
  std::unique_ptr<AbstractExecutor> right_child_;
  /** The hash table is just a map from join keys to a set of tuples */
  std::unordered_map<JoinKey, std::vector<Tuple>> ht_{};
  /** Buffer for tuples with duplicate join key */
  std::vector<Tuple> duplicate_buffer_;
  /** The index of current value to output from buffer */
  uint32_t buffer_idx_{0};
};

}  // namespace bustub
