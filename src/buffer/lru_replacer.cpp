//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_size_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock latch(mutex_);
  if (lru_replacer_.empty()) {
    frame_id = nullptr;
    return false;
  }
  *frame_id = lru_replacer_.back();
  lru_hash_.erase(lru_replacer_.back());
  lru_replacer_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock latch(mutex_);
  auto iter = lru_hash_.find(frame_id);
  if (iter == lru_hash_.end()) {
    return;
  }

  lru_replacer_.erase(iter->second);
  lru_hash_.erase(iter);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock latch(mutex_);
  if (lru_hash_.find(frame_id) != lru_hash_.end()) {
    return;
  }
  if (lru_replacer_.size() >= max_size_) {
    return;
  }
  lru_replacer_.push_front(frame_id);
  lru_hash_[frame_id] = lru_replacer_.begin();
}

size_t LRUReplacer::Size() {
  std::scoped_lock latch(mutex_);
  return lru_replacer_.size();
}

}  // namespace bustub
