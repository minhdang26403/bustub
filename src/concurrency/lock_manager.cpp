//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  TransactionState state = txn->GetState();
  auto isolation_level = txn->GetIsolationLevel();

  // Safety: Aborted transaction can't request any lock
  if (state == TransactionState::ABORTED) {
    return false;
  }
  // Doesn't allow REPEATABLE_READ to get lock in the shrinking phase
  if (isolation_level == IsolationLevel::REPEATABLE_READ && state == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // READ_UNCOMMITED doesn't need shared lock to read
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // The transaction already acquired lock
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> lock_table_latch(latch_);
  txn_id_t txn_id = txn->GetTransactionId();
  LockRequestQueue &request_queue = lock_table_[rid];

  // Wound-wait
  PreemptsYoungerRequests(&request_queue, txn_id, LockMode::SHARED);
  PreemptsYoungerExclusiveLockHolders(&request_queue, txn_id);

  // Transactions can always have shared locks since they only read committed values
  // At the time a transaction is committed, all its locks are released.
  request_queue.shared_lock_holders_.insert(txn_id);
  txn->GetSharedLockSet()->emplace(rid);

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  TransactionState state = txn->GetState();
  if (state == TransactionState::ABORTED) {
    return false;
  }

  // Don't grant exclusive lock in shrinking phase to prevent dirty writes
  if (state == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->IsSharedLocked(rid)) {
    return LockUpgrade(txn, rid);
  }

  std::unique_lock<std::mutex> lock_table_latch(latch_);
  txn_id_t txn_id = txn->GetTransactionId();
  LockRequestQueue &request_queue = lock_table_[rid];

  // Wound-wait deadlock prevention policy
  PreemptsYoungerRequests(&request_queue, txn_id, LockMode::EXCLUSIVE);
  PreemptsYoungerSharedLockHolders(&request_queue, txn_id);
  PreemptsYoungerExclusiveLockHolders(&request_queue, txn_id);

  if (!request_queue.request_queue_.empty() || !request_queue.shared_lock_holders_.empty() ||
      request_queue.exclusive_lock_holder_ != INVALID_TXN_ID) {
    request_queue.request_queue_.emplace_back(txn_id, LockMode::EXCLUSIVE);
    while (!request_queue.IsLockGranted(txn_id) && txn->GetState() != TransactionState::ABORTED) {
      request_queue.cv_.wait(lock_table_latch);
    }
  } else {
    request_queue.exclusive_lock_holder_ = txn_id;
  }

  // Wounded by another transaction while waiting
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  TransactionState state = txn->GetState();
  if (state == TransactionState::ABORTED) {
    return false;
  }
  // Can't upgrade lock in shrinking phase
  if (state == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }

  std::unique_lock<std::mutex> lock_table_latch(latch_);
  LockRequestQueue &request_queue = lock_table_[rid];
  if (request_queue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  txn_id_t txn_id = txn->GetTransactionId();
  request_queue.shared_lock_holders_.erase(txn_id);

  PreemptsYoungerRequests(&request_queue, txn_id, LockMode::EXCLUSIVE);
  PreemptsYoungerSharedLockHolders(&request_queue, txn_id);
  PreemptsYoungerExclusiveLockHolders(&request_queue, txn_id);

  if (request_queue.exclusive_lock_holder_ == INVALID_TXN_ID && request_queue.shared_lock_holders_.empty()) {
    request_queue.exclusive_lock_holder_ = txn_id;
  } else {
    request_queue.request_queue_.emplace_back(txn_id, LockMode::EXCLUSIVE);
    request_queue.upgrading_ = txn_id;
    while (!request_queue.IsLockGranted(txn_id) && txn->GetState() != TransactionState::ABORTED) {
      request_queue.cv_.wait(lock_table_latch);
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock_table_latch(latch_);
  auto isolation_level = txn->GetIsolationLevel();
  // Only change the state of REPEATABLE_READ ?
  if (isolation_level == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn_id_t txn_id = txn->GetTransactionId();
  LockRequestQueue &request_queue = lock_table_[rid];
  if (request_queue.exclusive_lock_holder_ == txn_id) {
    request_queue.exclusive_lock_holder_ = INVALID_TXN_ID;
  }
  auto shared_lock_iter = request_queue.shared_lock_holders_.find(txn_id);
  if (shared_lock_iter != request_queue.shared_lock_holders_.end()) {
    request_queue.shared_lock_holders_.erase(shared_lock_iter);
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // Process queued requests while this transaction holds lock
  if (request_queue.shared_lock_holders_.empty() && !request_queue.request_queue_.empty()) {
    ProcessQueue(&request_queue);
  }
  request_queue.cv_.notify_all();
  return true;
}

void LockManager::ProcessQueue(LockRequestQueue *request_queue) {
  LockRequest &lock_request = request_queue->request_queue_.front();
  request_queue->exclusive_lock_holder_ = lock_request.txn_id_;
  if (request_queue->upgrading_ == lock_request.txn_id_) {
    request_queue->upgrading_ = INVALID_TXN_ID;
  }
  request_queue->request_queue_.pop_front();
}

void LockManager::PreemptsYoungerRequests(LockRequestQueue *request_queue, txn_id_t txn_id, LockMode lock_mode) {
  auto request_iter = request_queue->request_queue_.begin();
  while (request_iter != request_queue->request_queue_.end()) {
    if ((lock_mode == LockMode::EXCLUSIVE ||
         (lock_mode == LockMode::SHARED && request_iter->lock_mode_ == LockMode::EXCLUSIVE)) &&
        txn_id < request_iter->txn_id_) {
      // Transaction waiting in the queue to be processed
      Transaction *queued_transaction = TransactionManager::GetTransaction(request_iter->txn_id_);
      queued_transaction->SetState(TransactionState::ABORTED);
      request_iter = request_queue->request_queue_.erase(request_iter);
    } else {
      ++request_iter;
    }
  }
}

void LockManager::PreemptsYoungerSharedLockHolders(LockRequestQueue *request_queue, txn_id_t txn_id) {
  auto lock_holder_id_iter = request_queue->shared_lock_holders_.begin();
  while (lock_holder_id_iter != request_queue->shared_lock_holders_.end()) {
    if (txn_id < *lock_holder_id_iter) {
      Transaction *shared_lock_holder = TransactionManager::GetTransaction(*lock_holder_id_iter);
      shared_lock_holder->SetState(TransactionState::ABORTED);
      lock_holder_id_iter = request_queue->shared_lock_holders_.erase(lock_holder_id_iter);
    } else {
      ++lock_holder_id_iter;
    }
  }
}

void LockManager::PreemptsYoungerExclusiveLockHolders(LockRequestQueue *request_queue, txn_id_t txn_id) {
  txn_id_t lock_holder_id = request_queue->exclusive_lock_holder_;
  if (txn_id < lock_holder_id) {
    Transaction *exclusive_lock_holder = TransactionManager::GetTransaction(lock_holder_id);
    exclusive_lock_holder->SetState(TransactionState::ABORTED);
    request_queue->exclusive_lock_holder_ = INVALID_TXN_ID;
  }
}

}  // namespace bustub
