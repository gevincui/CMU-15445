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

using namespace std;

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity = num_pages;
  head = new ListNode();
  tail = new ListNode();
  head->next = tail;
  tail->prev = head;
}

LRUReplacer::~LRUReplacer() {
  while(head->next != tail) {
    ListNode* nxt = head->next;
    head->next = head->next->next;
    delete nxt;
  }
  delete head;
  delete tail;
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  lru_latch.lock();
  if (lru_map.empty()) {  // lru中为空
    lru_latch.unlock();
    return false;
  }
  ListNode* delframe = tail->prev;
  frame_id_t delframe_id = delframe->frame_id;
  *frame_id = delframe_id;        // 被删除的数据页作为参数返回
  lru_map.erase(delframe_id);  // 从lru中删除这个数据页
  RemoveFrame(delframe);
  delete delframe;       // 释放资源
  lru_latch.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {   // 被Pin的数据页不能参与LRU，要从中移除
  lru_latch.lock();
  if (lru_map.count(frame_id) != 0) {
    ListNode* delframe = lru_map[frame_id];
    lru_map.erase(frame_id);    // 从lru_map移除
    RemoveFrame(delframe);     // 从双向队列中移除
    delete delframe;          // 释放资源
  }
  lru_latch.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {    // 被Pin的数据页，如果不在lru_map，则加入lru_map中
  lru_latch.lock();
  if (lru_map.count(frame_id) == 0) {
    ListNode* frame = new ListNode(frame_id);
    if (lru_map.size() >= capacity) {     // 当前lru_map已满，先淘汰一个再进去
      frame_id_t fid;
      Victim(&fid);
    }
    lru_map[frame_id] = frame;
    AddToHead(frame);
  }
  lru_latch.unlock();
}

auto LRUReplacer::Size() -> size_t { return lru_map.size(); }

void LRUReplacer::RemoveFrame(ListNode* frame) {
  frame->prev->next = frame->next;
  frame->next->prev = frame->prev;
}

void LRUReplacer::AddToHead(ListNode* frame) {
  frame->next = head->next;
  head->next->prev = frame;
  head->next = frame;
  frame->prev = head;
}

}  // namespace bustub