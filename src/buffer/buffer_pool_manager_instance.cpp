//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {
}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;

}

// 刷盘
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  if (page_table_.count(page_id) == 0 || page_id == INVALID_PAGE_ID) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
  return true;
}

// 将buffer pool全部数据页刷盘
void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
    disk_manager_->WritePage(it->first, pages_[page_table_[it->first]].data_);
  }
}

// 磁盘新建数据页，并读入内存
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  latch_.lock();
  // step0: 新建磁盘数据页(一个仅分配page_id的空页)
  *page_id = AllocatePage();

  // step1: 如果所有在buffer pool的数据页被pinned了，直接返nullptr
  bool all_pinned = true;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].pin_count_ == 0) {
      all_pinned = false;
      break;
    }
  }
  if(all_pinned) {
    latch_.unlock();
    return nullptr;
  }

  // step2:
  // 如果free list不为空(buffer pool有可用frame)，使用free list中的一个frame存放数据页；
  // 如果free list为空(buffer pool没有可用frame)，使用lru_replacer淘汰一个数据页；
  Page* p;
  frame_id_t frame_id;
  if (!free_list_.empty()) {    // buffer pool有可用的frame
    frame_id = free_list_.front();
    free_list_.pop_front();
    p = &pages_[frame_id];          // 拿到buffer pool未使用frame对应的空Page
  } else {        // buffer pool没有可用的frame
    if (!replacer_->Victim(&frame_id)) {   // 使用lru_replacer淘汰一个数据页
      latch_.unlock();
      return nullptr;
    }
    p = &pages_[frame_id];         // 拿到buffer pool中被淘汰的frame对应的Page
    if (p->is_dirty_) {              // 如果淘汰的是脏页，要刷盘
        disk_manager_->WritePage(p->page_id_, p->data_);
        p->is_dirty_ = false;
    }
    page_table_.erase(p->page_id_);
  }

  // step3: 初始化新数据页的信息：元数据、数据、page_table(frame_id映射到新的page_id上)
  // 当前线程正在使用该页，需要pined
  replacer_->Pin(frame_id);
  // 元数据
  p->page_id_ = *page_id;
  p->pin_count_ = 1;
  // 数据
  p->ResetMemory();      // 新建的数据页为空页，清空数据即可
  // page_table_
  page_table_[*page_id] = frame_id;

  // step4: 返回新建的page
  latch_.unlock();
  return p;
}

// 获取数据页
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  latch_.lock();
  frame_id_t frame_id;

  // step1: 从page_table找到buffer pool的page地址
  // step1.1: 如果找到，则直接返回
  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;        // 当前线程正在使用该页，需要pined
    replacer_->Pin(frame_id);
    latch_.unlock();
    return &pages_[frame_id];
  }

  // step1.2: 如果没找到，说明page在磁盘中
  // 如果free list不为空(buffer pool有可用frame)，使用free list中的一个frame存放数据页
  // 如果free list为空(buffer pool没有可用frame)，使用lru_replacer淘汰一个数据页
  Page* p;
  if (!free_list_.empty()) {    // buffer pool有可用的frame
    frame_id = free_list_.front();
    free_list_.pop_front();
    p = &pages_[frame_id];          // 拿到buffer pool未使用frame对应的空Page
  } else {        // buffer pool没有可用的frame
    if (!replacer_->Victim(&frame_id)) {   // 使用lru_replacer淘汰一个数据页
      latch_.unlock();
      return nullptr;
    }
    p = &pages_[frame_id];     // 拿到buffer pool中被淘汰的frame对应的Page
    if (p->is_dirty_) {              // step2: 如果淘汰的是脏页，要刷盘
      disk_manager_->WritePage(p->page_id_, p->data_);
      p->is_dirty_ = false;
    }
    page_table_.erase(p->page_id_);
  }

  // step3、step4: 初始化新数据页的信息：元数据、数据、page_table(frame_id映射到新的page_id上)
  // 当前线程正在使用该页，需要pined
  replacer_->Pin(frame_id);
  // step3: page_table
  page_table_[page_id] = frame_id;
  // step4： 元数据、数据
  p->page_id_ = page_id;
  p->pin_count_ = 1;
  disk_manager_->ReadPage(page_id, p->data_);

  latch_.unlock();
  return p;
}

// 删除磁盘数据页，buffer pool如果存在也要删除
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 2.   If P does not exist, return true.
  // 3.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 4.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  latch_.lock();
  // step0: 删除磁盘数据页
  DeallocatePage(page_id);

  // step1: 从page_table找到buffer pool的page地址
  // step2: 如果没找到，说明删除操作结束，直接返true
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return true;
  }


  frame_id_t frame_id = page_table_[page_id];
  Page* deletepage = &pages_[frame_id];      // buffer pool中应该被删除的page

  // step3: 如果找到，但是page正在被pinned，则此时不能删除，直接返false
  if (deletepage->pin_count_ != 0) {
    latch_.unlock();
    return false;
  }

  // step4: 如果找到，且page没有被pinned，更新buffer pool对应frame上的page信息：元数据、数据、page_table(将这个page_id的映射删除)，更新free list信息：加入该(frame_id)
  // page_table
  page_table_.erase(page_id);
  // 元数据
  deletepage->page_id_ = INVALID_PAGE_ID;
  deletepage->is_dirty_ = false;
  // free list
  free_list_.push_back(frame_id);
  // lru_repalacer不用处理，因为被删除的page对应的frame会加入到free list
  // 当需要使用buffer pool存放数据页时会优先使用free list(buffer pool中未使用的frame)，buffer pool用完了这个frame也一定在lru中了

  latch_.unlock();
  return false;
}

// 如果数据页不再被pinned，需要unpinned操作
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();

  frame_id_t frame_id = page_table_[page_id];
  Page* p = &pages_[frame_id];
  p->is_dirty_ = is_dirty;
  if (p->pin_count_ == 0)
    return false;
  --p->pin_count_;
  if (p->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
    latch_.unlock();
    return true;
  }

  latch_.unlock();
  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
