//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {

  // 查找应该包含key的叶子数据页
  Page *leaf_page = FindLeafPageByOperation(key, Operation::FIND, transaction).first;
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 再到应该包含key的叶子数据页查找有没有这个key
  ValueType value;
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);

  // 读操作结束，要释放锁住的叶子数据页
  leaf_page->RUnlatch();
  // page用完了unnpined对应的frame
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

  // 如果不存在，则直接退出
  if (!is_exist) {
    return false;
  }
  // 把结果放到result中
  result->push_back(value);
  return is_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {

  // 因为可能发生根节点重置，因此要锁住root page id，防止并发更新
  root_latch_.lock();
  if (IsEmpty()) {                 // 如果是空树，创建一个叶子数据页作为根数据页
    StartNewTree(key, value);
    root_latch_.unlock();
    return true;
  }
  root_latch_.unlock();
  return InsertIntoLeaf(key, value, transaction);    // 否则将它插入叶子数据页中

}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 磁盘新建数据页并读入buffer pool，新的page id作为root page id
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);  // 对应的frame会被pinned住
  // 将元数据(索引字段名+root page id)插入header page
  UpdateRootPageId(1);  // insert root page id in header page

  // 将数据插入到buffer pool中新建的数据页中
  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());
  root_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root_node->Insert(key, value, comparator_);

  // 用完了该frame，需要unpinned
  // 因为还没有刷盘，所以需要设置为脏页
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
/*
 * InsertIntoLeaf：完整的插入操作流程，如果遇到需要分裂的情况，会进行分裂
 * 对于Insert方法：
 * 1）所有上page锁的操作都是在第一步Find完成的
 * 2）叶子数据页外的祖先数据页的page锁在InsertIntoParent的递归结束时全部unlock
 * 3）叶子数据页的page锁在InsertIntoLeaf方法末尾unlock
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {

  // 寻找key应该在的叶子数据页，过程给需要加锁的数据页加锁
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::INSERT, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 判断key在不在应该在的叶子数据页
  ValueType v;
  bool exist = leaf_node->Lookup(key,&v,comparator_);

  // 如果存在，唯一键冲突，将所有被锁住的page解锁，被pinned的frame进行uppined，
  if (exist) {
    if (root_is_latched) {  // 如果root page id被锁住，也要解锁(根节点进行插入操作)
      root_latch_.unlock();
    }
    // 叶子数据页的祖先数据页unlock、unpinned
    UnlockUnpinPages(transaction);
    // 用完了叶子数据页对应的frame，需要unpinned，page要进行unlock
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  // 如果不存在，将key插入到应该插入的叶子数据页
  leaf_node->Insert(key,value,comparator_);

  // 如果插入后，叶子数据页的索引值个数达到max size(设计之初为了优雅，最多能插入max size - 1)
  // 需要进行叶子数据页分裂
  if (leaf_node->GetSize() >= leaf_node->GetMaxSize()) {
    LeafPage *new_leaf_page = Split(leaf_node);
    bool *pointer_root_is_latched = new bool(root_is_latched);
    InsertIntoParent(leaf_node,new_leaf_page->KeyAt(0),new_leaf_page,transaction,pointer_root_is_latched);
    delete pointer_root_is_latched;
    // Split会给新分裂出的数据页对应的frame进行pinned，不进行任何unpinned
    // InsertIntoParent会给父数据页进行1次pinned1次unpinned(如果新创建了根数据页，也会进行1次pinned1次unpinned)
    // 所以新分裂出来的数据页和旧数据页对应的frame都没有unpinned
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  }
  // 如果插入后，叶子数据页不需要分裂
  if (root_is_latched) {    // 如果root page id被锁住，也要解锁(根节点进行插入操作)
    root_latch_.unlock();
  }
  // 用完了叶子数据页对应的frame，需要unpinned，page要进行unlock
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);

  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */

// 新建一个数据页，将旧数据页一半的数据移动到新数据页
// 如果是叶子数据页，还要更新链表指针
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  // 磁盘新建数据页并读入buffer pool（分裂后新建的数据页）
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);  // 分裂出的新数据页对应的frame要pinned
  if (nullptr == new_page) {
    throw std::runtime_error("out of memory");
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->SetPageType(node->GetPageType());

  if (node->IsLeafPage()) {   // 如果旧数据页是叶子数据页
    LeafPage *old_leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    // 新叶子数据页指向与旧叶子数据页一样的父叶子数据页
    new_leaf_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    // 旧叶子数据页右半部分移动至新叶子数据页
    old_leaf_node->MoveHalfTo(new_leaf_node);
    // 更新叶子层的链表
    new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());
    old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());
  } else {
    InternalPage *old_internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(new_node);
    // 新非叶子数据页指向与旧非叶子数据页一样的父叶子数据页
    new_internal_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    // 旧非叶子数据页右半部分移动至新非叶子数据页
    old_internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
  }

  // 此时new node还没有unpinned，因为下面的InsertIntoPerent还要继续用
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */

// 叶子数据页分裂后，将分裂后的中间key(分裂后的新数据的第一个key)插入到父数据页中，value为分裂后的新数据页的page id
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction, bool *root_is_latched) {
  // 如果旧数据页是根数据页，那么需要树高会+1，新建一个数据页作为根数据页
  if (old_node->IsRootPage()) {
    // 磁盘新建数据页，并读入内存
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_page_id);  // 新建的根数据页对应的frame要pinned
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    // 更新B+数的根数据页id
    root_page_id_ = new_page_id;
    // 修改新的根结点的孩子指针，左指针指向旧数据页，新指针指向分裂出的新数据页
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 修改旧数据页和分裂出的新数据页的父指针
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    // 索引树的根数据页发生了变化，更新header page信息
    UpdateRootPageId(0);
    // 新的root必定不在transaction的page set队列中
    if (*root_is_latched) {   // 此时root page id更新完了，可以解锁
      *root_is_latched = false;
      root_latch_.unlock();
    }
    // 把之前锁住的页全部unlock
    UnlockPages(transaction);
    // 根数据页用完了，对应的frame要unpinned，并且修改了
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);  // 修改了根数据页的内容，设置为脏页
    return;
  }
  /*
   * 如果旧数据页不是根数据页，则要找它的父数据页
   * 将分裂后的中间key(分裂后的新数据的第一个key)插入到父数据页中，value为分裂后的新数据页的page id
   * 如果分裂的是叶子数据页: 相当于把分裂后的新数据页的第一个key存到上一层父数据页
   *     7                 5 7
   *    /        --->    /  ｜
   *  1 3 6            1 3  5 6
   * 如果分裂的是非叶子数据页：相当于把分裂后的新数据页的第一个key存到上一层父数据页，但是分裂后的数据页的第一个key弃用，正好满足B+树规范
   *    7                 5 7
   *   /        --->    /  |
   * 1 3 6            1 3  6
   *
   */
  // 找到旧数据页的父数据页
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());  // 父数据页对应的frame要pinned掉
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 把key插到父数据页应该在的位置，并且value指向分裂出的新数据页
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // 父节点未满，直接将root page id解锁，将除了叶子结点以外的数据页解锁，将
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }
    // 到达递归终点，将除了叶子数据页的其它祖先数据页解锁
    UnlockPages(transaction);
    // 父数据页用完了，对应的frame unpinned掉
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  // 父结点已满，需要Split拆分
  if (parent_node->GetSize() >= parent_node->GetMaxSize()) {
    InternalPage *new_parent_node = Split(parent_node);  // 分裂出的新父数据页会被pinned
    // 再递归InsertIntoParent
    InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);
    buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);  // 分裂出的新父数据页对应的frame要unpinned
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);      // 旧父数据页对应的frame要unpinned
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

/*
 * Remove：完整的删除操作流程，可能会遇到需要合并或向兄弟数据页借k-v的情况
 * 对于Delete方法：
 * 1）上page锁的操作一般情况都是在第一步Find完成的，如果发生向兄弟数据页借k-v，则在CoalesceOrRedistribute给兄弟数据页也要上锁
 * 2）叶子数据页外的祖先数据页的page锁在CoalesceOrRedistribute的方法结束时(Coalesce递归结束时)全部unlock
 * 3）叶子数据页的page锁在Remove方法末尾unlock
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 如果B+树是空树，直接返回
  if (IsEmpty()) {
    return;
  }
  // 找到需要进行删除操作的叶子数据页
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::DELETE, transaction);  // left_page对应的frame被pinned
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();

  // 在leaf page中删除key-value
  // 如果key存在，则size - 1，对应key-value被删除
  // 如果key不存在，则size不变
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);

  // 如果key不存在，直接退出，所有上锁的东西都解锁
  if (new_size == old_size) {
    if (root_is_latched) {
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // left_page对应的frame被unpinned
    return;
  }

  // 如果key存在，就调用CoalesceOrRedistribute(合并或向兄弟借k-v)，调整B+树为正确的结构
  bool *pointer_root_is_latched = new bool(root_is_latched);
  bool leaf_should_delete = CoalesceOrRedistribute(leaf_node, transaction, pointer_root_is_latched);
  delete pointer_root_is_latched;

  // 调用CoalesceOrRedistribute过程中，出现下面两种情况，需要删除该叶子数据页：
  // 1）该叶子数据页是根节点并且key被删光
  // 2）该叶子数据页执行了合并(Coalesce)
  // 该叶子数据页的祖先数据页的调整在CoalesceOrRedistribute的递归过程中完成
  if (leaf_should_delete) {
    transaction->AddIntoDeletedPageSet(leaf_page->GetPageId());
  }
  leaf_page->WUnlatch();      // 给叶子数据页unlock、uppined，准备删除
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  // 把所有需要删除的数据页都删除掉(上面调用CoalesceOrRedistribute方法中可能会向page set加入要删除的数据页)
  for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */

/*
 * 1）判断删除操作后的数据页size >= min size，来决定删除操作后，是否需要对B+树结构进行调整
 * 2）判断（兄弟数据页size + 执行删除操作后的数据页size）>= max size，来决定使用合并(Coalesce)还是向兄弟借k-v(Redistribute)
 * 如果是合并操作，则当前叶子页的k-v会被合并到兄弟数据页中，当前数据页要被删除
 * 如果是借向兄弟借k-v操作，则当前数据页不会被删除
 *
 */

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction, bool *root_is_latched) -> bool {

  // 如果删除操作后的数据页为根节点
  // 进入这个判断逻辑的两种场景：
  // 1）叶子数据页就是根数据页
  // 2）从叶子数据页开始，一直发生Coalesce，一直导致父数据页size不足，父数据页为根数据页，进入这个判断
  if (node->IsRootPage()) {
    bool root_should_delete = AdjustRoot(node);

    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }
    UnlockPages(transaction);

    return root_should_delete;
  }



  // 如果删除后的数据页size >= min size，不需要合并或向兄弟借k-v，直接返回false
  if (node->GetSize() >= node->GetMinSize()) {
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }
    UnlockPages(transaction);   // Coalesce方法的递归终点，将祖先数据页的所有page锁解锁

    return false;
  }

  // 如果删除后的数据页size < min size，则需要合并或向兄弟借k-v
  // 获取删除操作后的数据页的父数据页
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 获得删除操作后的数据页在父数据页的孩子指针(value)的index
  int index = parent->ValueIndex(node->GetPageId());
  // 根据index，寻找前一个兄弟数据页(如果index为0，再去找后一个兄弟数据页)
  page_id_t sibling_page_id = parent->ValueAt(index == 0 ? 1 : index - 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  // 记得要锁住兄弟数据页
  sibling_page->WLatch();

  // 如果(兄弟数据页size + 执行删除操作后的数据页size) <= (max size - 1)，使用Coalesce
  // 因为为了insert实现优美，留了一个预留位，一个最大容量为max size的数据页实际最大容量为max size - 1
  // 若一个数据页能支持两个数据页的合并，优先选择合并
  if (node->GetSize() + sibling_node->GetSize() <= node->GetMaxSize() - 1) {

    // Coalesce合并两个数据页到兄弟数据页
    bool parent_should_delete = Coalesce(&sibling_node, &node, &parent, index, transaction, root_is_latched);  // 返回值是父数据页是否需要被删除

    if (parent_should_delete) {
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
    }

    // 给兄弟数据页解锁
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

    return true;  // 合并，删除操作后的数据页需要被删除
  }

  // 若一个数据页不能支持两个数据页的合并，再选择向兄弟数据页借k-v
  if (*root_is_latched) {
    *root_is_latched = false;
    root_latch_.unlock();
  }

  Redistribute(sibling_node, node, index);

  UnlockPages(transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

  return false;  // 向兄弟借k-v，删除操作后的数据页无需删除
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */

/*
 * Coalesce算法流程：
 * 1）保证neighbor_node在node前驱(node在parent的孩子指针下标为0时，再考虑找后继兄弟)
 * 2）如果是非叶子数据页，需要找到parent中node和neighbor之间的middleKey，下移middleKey到node[0].key；
 * 3）将node合并到neighbor尾部
 * 4）调用parent的Remove方法删除parent中的middleKey及指向node的指针(在array_的同一下标)
 * 示例1：非叶子数据页，max size = 4， min size = 2
 *      1  5                  1
 *      |  \        --->      |
 *     2 4  6 (X)            2 4 5 6
 *   .........           .........
 * 示例2：叶子数据页，max size = 4，min size = 2
 *      2   5                  2
 *        |  \        --->     ｜
 *       2 4  5 (X)           2 4 5
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool *root_is_latched) -> bool {
  // neighbor_node为兄弟数据页，node为删除操作后的数据页，parent为二者的父数据页，index为node在parent中的孩子指针(value)下标
  // 需要保证neighbor_node在node的前驱，如果index = 0则需要交换node和neighbor_node
  // key_index表示实际的node在parent中的孩子指针(value)下标
  int key_index = index;
  if (index == 0) {
    std::swap(neighbor_node, node);
    key_index = 1;
  }

  // 找到parent需要下移的那个key
  KeyType middle_key = (*parent)->KeyAt(key_index);

  if ((*node)->IsLeafPage()) {
    // 如果执行删除操作后的数据页是叶子数据页
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    // 将k-v全部移动到前驱的兄弟数据页
    leaf_node->MoveAllTo(neighbor_leaf_node);
    // 更新叶子数据页链表指针
    neighbor_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  } else {
    // 如果执行删除操作后的数据页是非叶子数据页
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    // 下移middleKey，并将k-v全部移动到前驱的兄弟数据页
    internal_node->MoveAllTo(neighbor_internal_node, middle_key, buffer_pool_manager_);
  }

  // 删除parent中的middleKey及它指向node的指针
  (*parent)->Remove(key_index);

  // parent中删除了kv对，如果此时parent < min size，则parent需要进行Coalesce或Redistribute
  // 如果parent发生了Coalesce，则parent需要被删除，所以需要递归调用CoalesceOrRedistribute
  return CoalesceOrRedistribute(*parent, transaction, root_is_latched);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */

/*
 * Redistribute算法流程：
 * 1. 如果执行删除操作后的数据页是叶子数据页
 * 1）保证neighbor_node在node前驱 (node在parent的孩子指针下标为0时，再考虑找后继兄弟)
 * 2）向兄弟数据页尾部借一个k-v，插入到自己的头部
 * 3）父亲向兄弟数据页的头部借一个key，插入到原来middleKey位置
 * 示例：max size = 4，min size = 2
 *       2   5                  2   4
 *         |   \        --->      ｜  \
 *       2 3 4  5 (X)           2 3    4 5
 *
 * 2. 如果执行删除操作后的数据页是非叶子数据页
 * 1）保证neighbor_node在node前驱 (node在parent的孩子指针下标为0时，再考虑找后继兄弟)
 * 2）删除操作后的数据页向兄弟数据页尾部借一个v，向父数据页借一个middleKey，插入到自己的头部
 * 3）父亲向兄弟数据页尾部借一个key，插入到原来middleKey位置
 * 示例：max size = 4， min size = 2
 *      1  5                     1  4
 *       |   \        --->        |   \
 *     2 3 4  6 (X)              2 3  5 6
 *   .............            .............
 *
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {

  // 先找到执行删除操作后的数据页的父数据页
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // index=0，则neighbor是node后继结点
  // index>0，则neighbor是node前驱结点
  if (node->IsLeafPage()) {
    // 如果执行删除操作后的数据页是叶子数据页
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);

    if (index == 0) {
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    } else {
      // 如果index>0，则兄弟数据页在它的前驱
      // 向兄弟数据页尾部借一个k-v，插入到自己的头部
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      // 父亲向兄弟数据页的头部借一个key，插入到原来middleKey位置
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    // 如果执行删除操作后的数据页是非叶子数据页
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);

    if (index == 0) {
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(1), buffer_pool_manager_);
      parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    } else {
      // 如果index>0，则兄弟数据页在它的前驱
      // 删除操作后的数据页向兄弟数据页尾部借一个v，向父数据页借一个middleKey，插入到自己的头部
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      // 父亲向兄弟数据页尾部借一个key，插入到原来middleKey位置
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */

/* 根节点需要删除的两种情况：
 *
 * Case 1: old_root_node是非叶子数据页，仅仅有一个INVALID的key和一个左孩子指针value，要把它的孩子更新成新的根数据页
 * 出现场景：前面执行过Coalesce操作，即CoalesceOrRedistribute函数发生了递归
 * 例如下图： max size = 4，min size = 2，叶子数据页7右边的k-v刚刚被删除，
 * 那么会发生Coalesce合并为一个数据页，父数据页对应key下移，此时根数据页没有key，只有一个左孩子指针
 *       5                      (invalid-key)
 *     /   \      --->            /
 *   1 3   7 (X)               1 3 5 7
 *
 * Case 2: old_root_node是叶子数据页，大小为0，没有任何k-v
 * 出现场景：前面已将叶子数据页最后一个k-v删除，当前树为空树
 *
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {

  // Case 1: old_root_node是非叶子数据页，仅仅有一个INVALID的key和一个左孩子指针value，要把它的孩子更新成新的根数据页
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    // 把它的左孩子更新为新的根数据页
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild();
    // 更新root page id
    root_page_id_ = child_page_id;
    // 根数据页发生变化，更新header page元数据
    UpdateRootPageId(0);
    // 将新的根数据页读入buffer pool
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);   // 新的根数据页对应的frame被pinned
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    // 更新新的根数据页的父指针为INVALID
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    // 新的根数据页不再使用，对应的frame可以被unpinned
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);

    return true;    // 注意，此时旧的根数据页还没有unppined，会在CoalesceOrRedistribute()进行unpinned
  }

  // Case 2: old_root_node是叶子数据页，大小为0，没有任何k-v
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    // 更新root page id
    root_page_id_ = INVALID_PAGE_ID;
    // 根数据页发生变化，更新header page元数据
    UpdateRootPageId(0);

    return true;   // 注意，此时旧的根数据页还没有unppined，会在Remove()进行unpinned
  }

  // Case 1 和 Case 2 之外的情况，根数据页不需要被删除，直接返回false
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Page *leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, true).first;
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Page *leaf_page = FindLeafPageByOperation(key, Operation::FIND).first;
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // 如果key存在，idx为key的slot下标，如果key不存在，idx为插入key应该在的slot下标
  int idx = leaf_node->KeyIndex(key, comparator_);
            leaf_page->GetPageId(), leaf_node->GetPageId();   // DEBUG
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // 找到最左的叶子数据页
  Page *leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, false, true).first;  // 当前叶子数据页会被pinned
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // 注意：此时当前叶子数据页没有unpin，会随着迭代器调用析构函数unpinned
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_node->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
// 如果leftMost为false，寻找包含key的叶子数据页
// 如果leftMost为true，寻找最左边的叶子数据页
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  return FindLeafPageByOperation(key, Operation::FIND, nullptr, leftMost).first;
}


/*
 * FindLeafPageByOperation：Find操作的完整加锁流程/ Delete和Insert操作的第一步查询过程的加锁流程
 * 注意：此方法不包含Delete操作发生合并/向兄弟借k-v/根节点重置需要上的锁
 *
 * 1.Page锁
 * 加锁规则：
 * 1）Find操作：加读锁
 * 从根数据页开始加写锁，如果当前数据页锁住，就把父数据页的读锁释放，最终只锁叶子数据页
 * 2）Delete/Insert操作：加写锁
 * 从根数据页开始加写锁，如果当前数据页锁住 + 当前数据页安全(不会造成父数据页更新：合并/向兄弟借k-v/分裂，以及祖先的迭代更新)，则可以给祖先数据页写锁释放
 *
 * 2.root page id锁
 * 防止出现情况：在FetchPage时刻还是根数据页，在给它上锁时已经不是根数据页
 * 加锁规则：
 * 1）Find操作：
 * 根数据页上好Page锁之后就可以释放root page id锁了
 * 2）Delete/Insert操作：
 * 如果根数据页是安全的(不会发生根数据页重置，左孩子数据页上位)，则根数据页上好Page锁之后就可以释放root page id锁了
 * 如果根数据页不是安全的(可能会发生根数据页重置)，则根数据页上好Page锁不能释放root page id锁，要判断寻找key叶子数据页的下层对应的数据页是否安全
 *   i)如果下一层对应的数据页安全，则根数据页不会重置，可以释放root page id锁
 *   ii）如果下一层对应的数据页不安全，而且是Delete操作，则可能发生合并(祖先迭代合并)导致父数据页key个数-1发生重置，不能释放root page id锁
 *
 */

INDEX_TEMPLATE_ARGUMENTS
std::pair<Page *, bool> BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation operation,
                                                                Transaction *transaction, bool leftMost,
                                                                bool rightMost) {

  // 先锁住root page id，防止root page id在T1和T2时刻之间的并发更新
  // 保证给根数据页上锁时，它仍为根数据页
  root_latch_.lock();
  bool is_root_page_id_latched = true;

  // ---------------- T1 时刻-------------------
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);      // 获取根数据页
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (operation == Operation::FIND) {
    // ---------------- T2 时刻------------------
    // 如果是读操作，则给根数据页上读锁
    page->RLatch();
    // 根数据页上完锁后，root page id就可以更新了
    is_root_page_id_latched = false;
    root_latch_.unlock();
  } else {
    // ---------------- T2 时刻------------------
    // 如果是更新操作，则给根数据页上写锁
    page->WLatch();
    // 如果根数据页安全，根数据页上完锁后，root page id就可以更新了
    if (IsSafe(node, operation)) {
      is_root_page_id_latched = false;
      root_latch_.unlock();
    }
    // 如果根数据页不安全，说明根节点可能会重置(左孩子数据页上位)，root page id仍要继续上锁防止并发更新
  }

  while (!node->IsLeafPage()) {
    // 父数据页
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(node);

    page_id_t current_node_page_id;
    if (leftMost) {
      current_node_page_id = parent_node->ValueAt(0);
    } else if (rightMost) {
      current_node_page_id = parent_node->ValueAt(parent_node->GetSize() - 1);
    } else {
      current_node_page_id = parent_node->Lookup(key, comparator_);
    }

    // 当前数据页
    auto current_page = buffer_pool_manager_->FetchPage(current_node_page_id);
    auto current_node = reinterpret_cast<BPlusTreePage *>(current_page->GetData());

    if (operation == Operation::FIND) {
      // 如果是读操作，给当前数据页加读锁后，当前数据页就可以释放了
      current_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      // 如果是更新操作，给当前数据页加读锁后，检查当前数据页是否安全
      current_page->WLatch();
      // 将当前数据页加入事务
      transaction->AddIntoPageSet(page);
      // 如果当前数据页是安全的，root page id锁还没有释放，则释放root page id锁
      // 此时可以说明根数据页一定不会重置，因为该安全的数据页不会发生合并/向兄弟借k-v/分裂，则该数据页之上的数据页同样也不会发生
      if (IsSafe(current_node, operation)) {
        if (is_root_page_id_latched) {
          is_root_page_id_latched = false;
          root_latch_.unlock();
        }
        // 如果当前数据页安全，则可以把祖先数据页的page锁释放，把pinned住的frame都unppined掉
        UnlockUnpinPages(transaction);
      }
    }

    page = current_page;
    node = current_node;
  }
  
  return std::make_pair(page, is_root_page_id_latched);
}

// 检查当前数据页执行删除或插入(合并、向兄弟借k-v、分裂、根节点重置)，会不会更新其它数据页，如果会则是不安全的
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsSafe(N *node, Operation op) {

  // 插入操作前，key个数 < max size - 1 一定不会发生分裂(为了设计优美有一个预留位)，是安全的
  if (op == Operation::INSERT) {
    //
    return node->GetSize() < node->GetMaxSize() - 1;
  }

  if (op == Operation::DELETE) {
    // 如果当前执行删除操作的是根数据页
    if(node->IsRootPage()) {
      // 且为叶子数据页，则当前数据页安全
      if(node->IsLeafPage()) {
        return true;
      }
      // 且为非叶子数据页，则key个数 > 2
      // 若key=2，第一个key为INVALID，删除一个key后无可用key，会将左孩子数据页变为新的根节点(根节点重置)，说明当前数据页不安全
      return node->GetSize() > 2;
    }
    // 删除操作前，key个数 > min size 一定不会发生合并或向兄弟数据页借k-v，是安全的
    return node->GetSize() > node->GetMinSize();
  }

  return true;
}

/* unlock all pages */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  // unlock 和 unpin 事务经过的所有parent page
  for (Page *page : *transaction->GetPageSet()) {  // 前面加*是因为page set是shared_ptr类型
    page->WUnlatch();
  }
  transaction->GetPageSet()->clear();  // 清空page set
}

/* unlock and unpin all pages */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  // unlock 和 unpin 事务经过的所有parent page
  for (Page *page : *transaction->GetPageSet()) {  // 前面加*是因为page set是shared_ptr类型
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  // 疑问：此处dirty为false还是true？
  }
  transaction->GetPageSet()->clear();  // 清空page set
}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */

// 输入0代表更新header page，即某个索引树的根数据页发生了变化
// 输入1代表插入header page，即某个索引树刚刚建立
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
