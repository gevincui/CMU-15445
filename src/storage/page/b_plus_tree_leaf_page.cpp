//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_= next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {

  // 使用二分查找找到第一个大于等于key的位置，这就是叶子数据页查找数据的方式
  // 如果key不存在，则返回它应该插入的位置
  int left = 0;       // key从下标0有效
  int right = GetSize() - 1;

  while(left <= right) {
    int mid = left + (right - left) / 2;
    if(comparator(KeyAt(mid), key) >= 0) {
      right = mid -1;
    } else {
      left = mid + 1;
    }
  }

  int target_index = left;
  return target_index;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & {
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int insert_index = KeyIndex(key, comparator);   // 找到第一个>=key的下标

  if (GetSize() != 0 && comparator(KeyAt(insert_index), key) == 0) {  // 如果找到=key的，说明插入重复的key，直接返回
    return GetSize();
  }

  // 数组下标>=insert_index的元素整体后移1位
  // [insert_index, size - 1] --> [insert_index + 1, size]
  for (int i = GetSize(); i > insert_index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[insert_index] = MappingType{key, value};  // insert pair
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
/* 发生叶子数据页分裂，保持中间Keu在右半部分
 * 若为奇数，中间Key上移，左少右多
 *                        5
 *            --->     /     \
 *  2 4 7 9          2 4    5 7 9
 * 若为偶数，中间第二个Key上移，左=右
 *                        6
 *            ---->     /    \
 * 2 4 5 7 9         2 4 5   6 7 9
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {

  // 需要移动的记录的起始下标
  int start_index = GetMinSize();  //  (0,1,2) start index is 1; (0,1,2,3) start index is 2;
  int move_num = GetSize() - start_index;
  recipient->CopyNFrom(array_ + start_index, move_num);    // 把要移除的元素复制到recipent叶子数据页中
  IncreaseSize(-move_num);     // 更新被移除的叶子数据页的size
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // 移入元素到当前array中
  std::copy(items, items + size, array_ + GetSize());  // [items,items+size)复制到该page的array末尾
  IncreaseSize(size);                                 // 更新移入后的叶子数据页的size
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const
    -> bool {
  int target_index = KeyIndex(key, comparator);                                  // 查找第一个>=key的的下标
  if (target_index == GetSize() || comparator(key, KeyAt(target_index)) != 0) {  // =key的下标不存在（只有>key的下标）
    return false;
  }
  *value = array_[target_index].second;  // value是传出参数
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int {
  int target_index = KeyIndex(key, comparator);                                  // 查找第一个>=key的的下标
  if (target_index == GetSize() || comparator(key, KeyAt(target_index)) != 0) {  // =key的下标不存在（只有>key的下标）
    return GetSize();
  }

  IncreaseSize(-1);            // =key的下标存在
  for (int i = target_index; i < GetSize(); i++) {
    array_[i] = array_[i + 1];
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array_, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // 复制当前叶子数据页第一个key给recipient
  recipient->CopyLastFrom(array_[0]);
  // 删除当前叶子数据页第一个key
  IncreaseSize(-1);
  for (int i = 0; i < GetSize(); i++) {
    array_[i] = array_[i + 1];
  }
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array_[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array_[GetSize() - 1]);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for (int i = GetSize(); i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  // insert item to array[0]
  array_[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
