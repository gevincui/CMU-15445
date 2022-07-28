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

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"


namespace bustub {

void LockManager::AbortImplicitly(Transaction *txn, AbortReason abort_reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
}

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  // 读未提交隔离级别只加写锁，不能加读锁
  // 不加读锁是因为直接读最新的数据(未提交的数据也算)
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    AbortImplicitly(txn, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  // 事务状态为SHRINKING，不能加锁(仅有可重复读，读未提交不加读锁)
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // 如果当前事务已经持有该rid对应tuple的读锁或写锁，则无需再申请
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  // 如果当前事务没拿到该rid对应的tuple的锁，则找出锁申请队列
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  // 把当前事务加入到该锁的申请队列中
  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::SHARED);

  // wait(mutex，pred), 如果pred == true则当前执行线程不阻塞，否则阻塞，即锁等待
  // cv用于控制锁等待，如果拿不到锁则阻塞在此处
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  // 事务被终止了
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // 获得读锁
  lock_request.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);

  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  // 读未提交隔离级别只加写锁，不加读锁
  // 不加读锁是因为直接读最新的数据(未提交的数据也算)

  // 事务状态为SHRINKING，不能加锁(可重复读、读未提交)
  if (txn->GetState() == TransactionState::SHRINKING) {  // ？
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // 如果当前事务已经持有该rid对应tuple的写锁，则无需再申请
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  // 找出锁申请队列
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  // 把当前事务加入到该锁的申请队列中
  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::EXCLUSIVE);

  // cv用于控制锁等待，如果拿不到锁则阻塞在此处
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  // 事务被终止了
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // 获得写锁
  lock_request.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  // 事务状态为SHRINKING，不能加锁(读未提交、可重复读)
  if (txn->GetState() == TransactionState::SHRINKING) {  // ？
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // 如果当前事务已经拿到rid对应tuple的写锁，则无需加锁
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  // 找出锁申请队列
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  /*
   * 如果已有其它事务正在进行锁升级，则该事务不能进行锁升级，如示例1
   * 为了避免死锁，所以将当前申请锁升级的事务回滚
   *--------------------------------------------------------------------------
   * 示例1：
   *       trx1             trx2
   * T1  申请rid1读锁     申请rid1读锁
   * T2  申请rid1写锁     申请rid1写锁
   *
   * 由图可知，T1时刻trx1、trx2都拿到rid1的读锁
   * 但T2时刻都要申请写锁，都需要等到对方释放读锁才能拿到(强2PL事务提交才释放锁)，产生死锁
   *---------------------------------------------------------------------------
   * 注意，虽然通过回滚可以解决上述死锁，但解决不了下述死锁问题
   * 示例2：
   *       trx1             trx2
   * T1  申请rid1读锁     申请rid2读锁
   * T2  申请rid2写锁     申请rid1写锁
   *
   * 这种死锁只能通过资源分配图是否有环来检测死锁并回滚
   *
   */

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  if (lock_request_queue.upgrading_ != INVALID_TXN_ID) {
    AbortImplicitly(txn, AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  // 标记当前事务id正在进行锁升级
  lock_request_queue.upgrading_ = txn->GetTransactionId();

  // 找到该事务已批准的读锁申请
  auto it = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });

  // 将读锁申请更新为写锁申请，把批准状态改为未批准
  it->lock_mode_ = LockManager::LockMode::EXCLUSIVE;
  it->granted_ = false;

  // cv用于控制锁等待，如果拿不到写锁则阻塞在此处
  // 需要等待其它事务都提交把读锁都释放，当前事务才能进行锁升级
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request = *it, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  // 事务被终止了
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // 锁升级：获得写锁，释放读锁
  it->granted_ = true;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  // 锁升级结束
  lock_request_queue.upgrading_ = INVALID_TXN_ID;

  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  // 找出该锁的等待队列
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);

  // 如果是可重复读或读未提交隔离级别，释放锁时则说明事务已提交，将事务状态更新为SHRINKING
  // 仅可重复读、读未提交隔离级别有SHRINKING状态，因为是强2PL
  if (txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // 找到该事务已批准的锁申请
  auto it = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });
  ;

  // 删除申请队列中的这个锁申请
  auto following_it = lock_request_queue.request_queue_.erase(it);

  // 释放该锁
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // cv唤醒所有锁等待的线程，再次尝试获取锁
  if (following_it != lock_request_queue.request_queue_.end() && !following_it->granted_ &&
      LockManager::IsLockCompatible(lock_request_queue, *following_it)) {
    lock_request_queue.cv_.notify_all();
  }

  return true;
}

// 事务t1在等待事务t2的资源，插入t1->t2的边
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // 找到以t1为起点的所有边
  auto &v = waits_for_[t1];
  // 找到t2应该插入的位置
  auto it = std::lower_bound(v.begin(), v.end(), t2);

  // 如果已经有t1->t2的边，直接返回
  if (it != v.end() && *it == t2) {
    return;
  }
  // 否则插入t1-t2的边
  v.insert(it, t2);
}

// 事务t1不再等待事务t2的资源，删除t1->t2的边
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // 找到以t1为起点的所有边
  auto &v = waits_for_[t1];
  // 找到t2的位置
  auto it = std::find(v.begin(), v.end(), t2);

  // 如果t2存在，则删除t1->t2
  if (it != v.end()) {
    v.erase(it);
  }
}

// 拿到waits-for graph
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> r;
  for (const auto &[txn_id, txn_id_v] : waits_for_) {
    std::transform(txn_id_v.begin(), txn_id_v.end(), std::back_inserter(r),
                   [&t1 = txn_id](const auto &t2) { return std::make_pair(t1, t2); });
  }
  return r;
}

// 检测waits-for graph是否有环
bool LockManager::HasCycle(txn_id_t *txn_id) {
  // 拿到所有边的起点
  std::vector<txn_id_t> vertices;
  std::transform(waits_for_.begin(), waits_for_.end(), std::back_inserter(vertices),
                 [](const auto &pair) { return pair.first; });
  // 给所有起点以事务id从小到大排序
  std::sort(vertices.begin(), vertices.end());

  // visited记录每个点是否被访问过
  std::unordered_map<txn_id_t, LockManager::VisitedType> visited;

  // 遍历所有边的起点
  for (auto &&v : vertices) {
    // 如果当前起点还没被访问过
    if (auto it = visited.find(v); it == visited.end()) {
      // 将当前起点加入栈中，状态置为IN_STACK
      std::stack<txn_id_t> stack;
      stack.push(v);
      visited.emplace(v, LockManager::VisitedType::IN_STACK);
      // 以当前栈顶为起点，判断是否有环
      auto has_cycle = ProcessDFSTree(txn_id, &stack, &visited);
      if (has_cycle) {
        return true;
      }
    }
  }
    return false;
}


// DFS：以当前栈顶为起点判断是否有环
// txn_id是需要回滚的事务id，stack用于实现dfs，visited用于判断当前点是否访问过
bool LockManager::ProcessDFSTree(txn_id_t * txn_id, std::stack<txn_id_t> * stack,
                                 std::unordered_map<txn_id_t, LockManager::VisitedType> * visited) {
  bool has_cycle = false;

  // 遍历以栈顶元素为起点的所有边的终点
  for (auto &&v : waits_for_[stack->top()]) {
    // 如果当前终点(当前边)访问过，且状态为IN_STACK(此次DFS的路线)，说明有环
    auto it = visited->find(v);
    if (it != visited->end() && it->second == LockManager::VisitedType::IN_STACK) {
      *txn_id = GetYoungestTransactionInCycle(stack, v);
      has_cycle = true;
      break;
    }

    // 如果当前终点(当前边)没被访问过，则加入栈中，并且状态置为IN_STACK(加入此次DFS的路线中)
    if (it == visited->end()) {
      stack->push(v);
      visited->emplace(v, LockManager::VisitedType::IN_STACK);
      // 以当前终点为起点，判断是否有环
      has_cycle = ProcessDFSTree(txn_id, stack, visited);
      if (has_cycle) {
        break;
      }
    }
  }

  // 当前栈顶是否有环已经判断结束，将当前栈顶标记为VISITED
  visited->insert_or_assign(stack->top(), LockManager::VisitedType::VISITED);
  // 清除栈顶
  stack->pop();

  return has_cycle;
}

// 发现有环时，拿到环内最年轻的事务(最大的事务id)
txn_id_t LockManager::GetYoungestTransactionInCycle(std::stack<txn_id_t> *stack, txn_id_t vertex) {
  txn_id_t max_txn_id = 0;
  std::stack<txn_id_t> tmp;
  tmp.push(stack->top());
  stack->pop();

  // 将环内的点到加入到tmp中
  while (tmp.top() != vertex) {
    tmp.push(stack->top());
    stack->pop();
  }

  // 找到环内最大的事务id
  while (!tmp.empty()) {
    max_txn_id = std::max(max_txn_id, tmp.top());
    stack->push(tmp.top());
    tmp.pop();
  }

  return max_txn_id;
}

// 构造Waits-For Graph
void LockManager::BuildWaitsForGraph() {
  // 遍历每一个rid的锁申请队列
  for (const auto &it : lock_table_) {
    const auto queue = it.second.request_queue_;
    // 拿着锁的事务
    std::vector<txn_id_t> holdings;
    // 等待锁的事务
    std::vector<txn_id_t> waitings;

    // 遍历锁申请队列的每一个事务
    for (const auto &lock_request : queue) {
      const auto txn = TransactionManager::GetTransaction(lock_request.txn_id_);
      if (txn->GetState() == TransactionState::ABORTED) {
        continue;
      }
      // 已经拿到锁的事务，加入holdings
      if (lock_request.granted_) {
        holdings.push_back(lock_request.txn_id_);
      } else { // 处于锁等待的事务，加入waitings
        waitings.push_back(lock_request.txn_id_);
      }
    }

    // 画waits-for graph
    for (auto &&t1 : waitings) {
      for (auto &&t2 : holdings) {
        AddEdge(t1, t2);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    // 周期性执行死锁检测(默认间隔50毫秒)
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      if (!enable_cycle_detection_) {
        break;
      }

      // 清空waits-for graph，重画
      waits_for_.clear();
      BuildWaitsForGraph();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        // 如果存在环，将环内最年轻事务终止
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);

        // 遍历该事务之前锁等待的事务
        for (const auto &wait_on_txn_id : waits_for_[txn_id]) {
          auto wait_on_txn = TransactionManager::GetTransaction(wait_on_txn_id);
          // 得到该事务锁等待的事务持有的全部锁(rid)
          std::unordered_set<RID> lock_set;
          lock_set.insert(wait_on_txn->GetSharedLockSet()->begin(), wait_on_txn->GetSharedLockSet()->end());
          lock_set.insert(wait_on_txn->GetExclusiveLockSet()->begin(), wait_on_txn->GetExclusiveLockSet()->end());
          // 唤醒每一个锁(rid)的申请队列，让那些处于锁等待的事务再次尝试拿一次锁
          // 例如：之前当前事务申请写锁没成功，处于锁等待，将后面申请读锁的事务也阻塞了，现在该事务终止了，重新唤醒这些事务再次尝试拿一次读锁
          for (auto locked_rid : lock_set) {
            lock_table_[locked_rid].cv_.notify_all();
          }
        }

        // 可能不止一个环，再次清空重画
        waits_for_.clear();
        BuildWaitsForGraph();
      }
    }
  }
}

}  // namespace bustub
