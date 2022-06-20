//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  directory_page->SetPageId(directory_page_id_);
  directory_page->IncrGlobalDepth();

  // Initially, global depth is 1 (table size is 2) and the number of buckets is 2
  page_id_t bucket_0_page_id = INVALID_PAGE_ID;
  page_id_t bucket_1_page_id = INVALID_PAGE_ID;
  buffer_pool_manager_->NewPage(&bucket_0_page_id);
  buffer_pool_manager_->NewPage(&bucket_1_page_id);

  directory_page->SetBucketPageId(0, bucket_0_page_id);
  directory_page->SetLocalDepth(0, 1);
  directory_page->SetBucketPageId(1, bucket_1_page_id);
  directory_page->SetLocalDepth(1, 1);

  buffer_pool_manager_->UnpinPage(bucket_0_page_id, false);
  buffer_pool_manager_->UnpinPage(bucket_1_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return buffer_pool_manager_->FetchPage(bucket_page_id);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::GetBucketPageData(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  auto directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  auto bucket_page = FetchBucketPage(bucket_page_id);

  bucket_page->RLatch();
  auto bucket_page_data = GetBucketPageData(bucket_page);
  bool is_key_exist = bucket_page_data->GetValue(key, comparator_, result);
  bucket_page->RUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);

  table_latch_.RUnlock();
  return is_key_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  auto directory_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  bucket_page->WLatch();
  auto bucket_page_data = GetBucketPageData(bucket_page);
  if (!bucket_page_data->IsFull()) {
    bool insert_succeed = bucket_page_data->Insert(key, value, comparator_);
    bucket_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, insert_succeed);
    table_latch_.RUnlock();
    return insert_succeed;
  }
  bucket_page->WUnlatch();
  table_latch_.RUnlock();

  table_latch_.WLock();
  // Bucket is full
  bucket_page->RLatch();
  if (!bucket_page_data->IsFull()) {
    bucket_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    return Insert(transaction, key, value);
  }
  bucket_page->RUnlatch();

  if (directory_page->GetGlobalDepth() == directory_page->GetLocalDepth(bucket_idx)) {
    uint32_t old_size = directory_page->Size();
    uint32_t new_size = old_size * 2;
    if (new_size > DIRECTORY_ARRAY_SIZE) {
      buffer_pool_manager_->UnpinPage(directory_page_id_, false);
      buffer_pool_manager_->UnpinPage(bucket_page_id, false);
      table_latch_.WUnlock();
      return false;
    }

    directory_page->IncrGlobalDepth();
    for (uint32_t cur_bucket_idx = old_size; cur_bucket_idx < new_size; cur_bucket_idx++) {
      directory_page->SetBucketPageId(cur_bucket_idx, directory_page->GetBucketPageId(cur_bucket_idx - old_size));
      directory_page->SetLocalDepth(cur_bucket_idx, directory_page->GetLocalDepth(cur_bucket_idx - old_size));
    }

    // Create a new bucket to hold the new key-value pair
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    auto new_bucket_page_data =
        reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_page_id)->GetData());

    directory_page->IncrLocalDepth(bucket_idx);
    uint32_t split_image_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
    directory_page->IncrLocalDepth(split_image_bucket_idx);
    directory_page->SetBucketPageId(split_image_bucket_idx, new_bucket_page_id);

    bucket_page->WLatch();
    for (uint32_t slot = 0; slot < BUCKET_ARRAY_SIZE; slot++) {
      if (bucket_page_data->IsReadable(slot)) {
        KeyType key = bucket_page_data->KeyAt(slot);
        ValueType value = bucket_page_data->ValueAt(slot);
        if (KeyToDirectoryIndex(key, directory_page) != bucket_idx) {
          new_bucket_page_data->Insert(key, value, comparator_);
          bucket_page_data->Remove(key, value, comparator_);
        }
      }
    }
    bucket_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_bucket_page_id, true);
  } else {
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    auto new_bucket_page_data =
        reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_page_id)->GetData());

    directory_page->IncrLocalDepth(bucket_idx);
    uint32_t local_depth = directory_page->GetLocalDepth(bucket_idx);
    uint32_t local_depth_mask = directory_page->GetLocalDepthMask(bucket_idx);
    uint32_t hash_table_size = directory_page->Size();

    for (uint32_t cur_bucket_idx = 0; cur_bucket_idx < hash_table_size; cur_bucket_idx++) {
      if (directory_page->GetBucketPageId(cur_bucket_idx) == bucket_page_id) {
        if ((cur_bucket_idx & local_depth_mask) != (bucket_idx & local_depth_mask)) {
          directory_page->SetBucketPageId(cur_bucket_idx, new_bucket_page_id);
        }
        directory_page->SetLocalDepth(cur_bucket_idx, local_depth);
      }
    }

    bucket_page->WLatch();
    for (uint32_t slot = 0; slot < BUCKET_ARRAY_SIZE; slot++) {
      if (bucket_page_data->IsReadable(slot)) {
        KeyType key = bucket_page_data->KeyAt(slot);
        ValueType value = bucket_page_data->ValueAt(slot);
        if (static_cast<page_id_t>(KeyToPageId(key, directory_page)) != bucket_page_id) {
          new_bucket_page_data->Insert(key, value, comparator_);
          bucket_page_data->Remove(key, value, comparator_);
        }
      }
    }
    bucket_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_bucket_page_id, true);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.WUnlock();

  // Reattempt to insert the new record
  return Insert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  auto bucket_page = FetchBucketPage(bucket_page_id);
  bucket_page->WLatch();
  auto bucket_page_data = GetBucketPageData(bucket_page);
  bool remove_succeed = bucket_page_data->Remove(key, value, comparator_);

  if (bucket_page_data->IsEmpty()) {
    bucket_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, remove_succeed);
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return remove_succeed;
  }
  bucket_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, remove_succeed);
  table_latch_.RUnlock();
  return remove_succeed;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  auto directory_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);
  uint32_t split_image_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  uint32_t bucket_local_depth = directory_page->GetLocalDepth(bucket_idx);
  uint32_t split_image_local_depth = directory_page->GetLocalDepth(split_image_bucket_idx);

  bucket_page->RLatch();
  auto bucket_page_data = GetBucketPageData(bucket_page);

  if (!bucket_page_data->IsEmpty() || bucket_local_depth <= 1 || bucket_local_depth != split_image_local_depth) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    bucket_page->RUnlatch();
    table_latch_.WUnlock();
    return;
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  bucket_page->RUnlatch();

  page_id_t split_image_bucket_page_id = directory_page->GetBucketPageId(split_image_bucket_idx);
  directory_page->DecrLocalDepth(split_image_bucket_idx);
  directory_page->DecrLocalDepth(bucket_idx);
  directory_page->SetBucketPageId(bucket_idx, split_image_bucket_page_id);
  buffer_pool_manager_->DeletePage(bucket_page_id);

  for (uint32_t i = 0; i < directory_page->Size(); i++) {
    if (directory_page->GetBucketPageId(i) == bucket_page_id ||
        directory_page->GetBucketPageId(i) == split_image_bucket_page_id) {
      directory_page->SetBucketPageId(i, split_image_bucket_page_id);
      directory_page->SetLocalDepth(i, directory_page->GetLocalDepth(bucket_idx));
    }
  }

  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
