//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
  };

  class LockRequestQueue {
   public:
    // Queue of unprocessed exclusive lock requests
    std::list<LockRequest> request_queue_;
    // Queue of transactions holding shared locks
    std::unordered_set<txn_id_t> shared_lock_holders_;
    // for notifying blocked transactions on this rid
    std::condition_variable cv_;
    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;
    // txn_id of the transaction holding exclusive lock
    txn_id_t exclusive_lock_holder_ = INVALID_TXN_ID;

    bool IsLockGranted(txn_id_t txn_id) const {
      return shared_lock_holders_.find(txn_id) != shared_lock_holders_.end() || exclusive_lock_holder_ == txn_id;
    }
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() = default;

  ~LockManager() = default;

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  /**
   * Preempts (aborts) younger requests incompatible with the current lock requester
   * @param request_queue The general request queue
   * @param txn_id the id of the transaction that requests the lock
   * @param lock_mode the type of lock it requests
   */
  void PreemptsYoungerRequests(LockRequestQueue *request_queue, txn_id_t txn_id, LockMode lock_mode);

  /**
   * Preempts (aborts) younger shared lock holders incompatible with the current lock requester
   * @param request_queue The general request queue
   * @param txn_id the id of the transaction that requests the lock
   */
  void PreemptsYoungerSharedLockHolders(LockRequestQueue *request_queue, txn_id_t txn_id);

  /**
   * Preempts (aborts) younger exclusive lock holders incompatible with the current lock requester
   * @param request_queue The general request queue
   * @param txn_id the id of the transaction that requests the lock
   */
  void PreemptsYoungerExclusiveLockHolders(LockRequestQueue *request_queue, txn_id_t txn_id);

  /**
   * Handle exclusive lock requests in the queue
   * @param request_queue The general request queue
   */
  void ProcessQueue(LockRequestQueue *request_queue);

 private:
  /** The latch protects the shared lock table */
  std::mutex latch_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
};

}  // namespace bustub
