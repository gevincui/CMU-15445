//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;



namespace bustub {

struct ListNode {
  frame_id_t frame_id;
  ListNode* prev;
  ListNode* next;
  ListNode():  prev(nullptr), next(nullptr) {}
  ListNode(frame_id_t frame_id): frame_id(frame_id), prev(nullptr), next(nullptr) {}
};

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  unordered_map<frame_id_t, ListNode*> lru_map;   // 用于快速定位数据页的map
  mutex lru_latch;
  ListNode* head;  // 双向队列的队头,dummy指针,存放最近使用的数据页,队尾存放最不精彩使用的数据页
  ListNode* tail;  // 双向队列的队尾,dummy指针
  size_t capacity;  // lru的容量

  void RemoveFrame(ListNode *frame);
  void AddToHead(ListNode *frame);
};

}  // namespace bustub
