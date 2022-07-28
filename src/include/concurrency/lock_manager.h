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
#include <stack>
#include <unordered_map>
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
  enum class VisitedType { NOT_VISITED, IN_STACK, VISITED };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
  };


  class LockRequestQueue {
   public:
    std::mutex latch_;
    // 所有申请锁的事务(批准的拿到了锁，未批准的处于锁等待状态)
    std::list<LockRequest> request_queue_;
    // 用于控制锁等待
    std::condition_variable cv_;
    // 正在锁升级的事务id
    txn_id_t upgrading_ = INVALID_TXN_ID;
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  explicit LockManager(bool enable_cycle_detection) {
    enable_cycle_detection_ = enable_cycle_detection;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    // 阻塞析构函数，直至死锁检测线程执行结束
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

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
  auto LockShared(Transaction *txn, const RID &rid) -> bool;

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  auto LockExclusive(Transaction *txn, const RID &rid) -> bool;

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockUpgrade(Transaction *txn, const RID &rid) -> bool;

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  auto Unlock(Transaction *txn, const RID &rid) -> bool;

  /**
   * 根据rid等待队列判断当前请求线程能否拿到锁(是否要进行锁等待)
   *
   * 申请读锁时，当且仅当以下情况时返回true：
   * 1）没有任何事务拿到锁
   * 2）此时所有事务拿到的都是读锁
   *
   * 申请写锁时，当且仅当以下情况时返回true:
   * 1) 没有任何事务拿到锁
   * 2）此时当前事务拿到读锁且其它事务没拿到锁(锁升级的情况)
   *
   * @param lock_request_queue the queue to test compatibility
   * @param to_check_request the request to test
   * @return true if compatible, otherwise false
   */
  static bool IsLockCompatible(const LockRequestQueue &lock_request_queue, const LockRequest &to_check_request) {

    // 申请的是写锁
    if(to_check_request.lock_mode_ == LockMode::EXCLUSIVE) {
      // 如果该申请位于队头时可以拿到写锁(此时没有任何事务拿到锁/锁升级场景)
      return lock_request_queue.request_queue_.begin()->txn_id_ == to_check_request.txn_id_;
    }

    // 申请的是读锁
    for (auto &&lock_request : lock_request_queue.request_queue_) {
      // 如果前面申请的都是读锁且都拿到了，则该申请也能拿到读锁(此时所有事务拿到的都是读锁)
      if (lock_request.txn_id_ == to_check_request.txn_id_) {
        return true;
      }
      // 判断前面申请的读锁是否都拿到了
      const auto is_compatible = lock_request.granted_ && lock_request.lock_mode_ == LockMode::SHARED;
      if (!is_compatible) {
        return false;
      }
    }

    // 不会走到此逻辑
    return true;
  }

  /*** 维护waits-for graph的 API ***/

  /** Adds an edge from t1 -> t2. */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /** Removes an edge from t1 -> t2. */
  void RemoveEdge(txn_id_t t1, txn_id_t t2);

  /**
   * 检查等待图是否有环
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  bool HasCycle(txn_id_t *txn_id);

  /** 得到waits-for graph */
  std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

  /** 开启死锁检测 */
  void RunCycleDetection();

  /** 关闭死锁检测 */
  void StopCycleDetection() { enable_cycle_detection_ = false; }

 private:
  // 将事务变为终止状态的函数
  void AbortImplicitly(Transaction *txn, AbortReason abort_reason);

  // DFS：以当前栈顶为起点判断是否有环
  bool ProcessDFSTree(txn_id_t *txn_id, std::stack<txn_id_t> *stack,
                      std::unordered_map<txn_id_t, VisitedType> *visited);
  // 得到环内最年轻的事务id
  txn_id_t GetYoungestTransactionInCycle(std::stack<txn_id_t> *stack, txn_id_t vertex);
  // 构造Wait-For Graph
  void BuildWaitsForGraph();


  // 控制安全并发操作lock_table_的锁
  std::mutex latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  // 维护一个map表示waits-for graph
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;

  // 维护每个rid的锁申请队列的map
  std::unordered_map<RID, LockRequestQueue> lock_table_;
};

}  // namespace bustub
