// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// bthread - A M:N threading library to make applications more concurrent.

// Date: Tue Jul 10 17:40:58 CST 2012

#ifndef BTHREAD_WORK_STEALING_QUEUE_H
#define BTHREAD_WORK_STEALING_QUEUE_H

#include "butil/macros.h"
#include "butil/atomicops.h"
#include "butil/logging.h"

namespace bthread {

template <typename T>
class WorkStealingQueue {
public:
    WorkStealingQueue()
        : _bottom(1)
        , _capacity(0)
        , _buffer(NULL)
        , _top(1) {
    }

    ~WorkStealingQueue() {
        delete [] _buffer;
        _buffer = NULL;
    }

    int init(size_t capacity) {
        if (_capacity != 0) {
            LOG(ERROR) << "Already initialized";
            return -1;
        }
        if (capacity == 0) {
            LOG(ERROR) << "Invalid capacity=" << capacity;
            return -1;
        }
        if (capacity & (capacity - 1)) {
            LOG(ERROR) << "Invalid capacity=" << capacity
                       << " which must be power of 2";
            return -1;
        }
        _buffer = new(std::nothrow) T[capacity];
        if (NULL == _buffer) {
            return -1;
        }
        _capacity = capacity;
        return 0;
    }

    // Push an item into the queue.
    // Returns true on pushed.
    // May run in parallel with steal().
    // Never run in parallel with pop() or another push().
    bool push(const T& x) {
        const size_t b = _bottom.load(butil::memory_order_relaxed);  // steal不会修改bottom，所以只用relaxed读
        const size_t t = _top.load(butil::memory_order_acquire); // 确保后面的指令不重排到前面
        if (b >= t + _capacity) { // Full queue.
            return false;
        }
        _buffer[b & (_capacity - 1)] = x;
        _bottom.store(b + 1, butil::memory_order_release);  // 确保steal能够看到bottom处写入的数据
        return true;
    }

    // Pop an item from the queue.
    // Returns true on popped and the item is written to `val'.
    // May run in parallel with steal().
    // Never run in parallel with push() or another pop().
    bool pop(T* val) {
        const size_t b = _bottom.load(butil::memory_order_relaxed);
        size_t t = _top.load(butil::memory_order_relaxed);
        if (t >= b) {  // 快速判断是否为空
            // fast check since we call pop() in each sched.
            // Stale _top which is smaller should not enter this branch.
            return false;
        }
        const size_t newb = b - 1;
        _bottom.store(newb, butil::memory_order_relaxed);
        butil::atomic_thread_fence(butil::memory_order_seq_cst);  // 保证同一元素不会既被pop，又被steal，也就是保证bottom在126行可以读到最新值
        t = _top.load(butil::memory_order_relaxed);
        if (t > newb) {  // 说明queue为空
            _bottom.store(b, butil::memory_order_relaxed);  // 将 bottom 修改回去
            return false;
        }
        *val = _buffer[newb & (_capacity - 1)];
        if (t != newb) {  // 如果t!=newb，说明队列中不止一个数据，此时pop与steal不会竞争，可以直接返回
            return true;
        }
        // Single last element, compete with steal()
        const bool popped = _top.compare_exchange_strong(
            t, t + 1, butil::memory_order_seq_cst, butil::memory_order_relaxed);  // 如果_top还等于t，则说明没有发生steal，此时将top和bottom都+1, pop的数据可用，否则发生了steal，应将bottom恢复，pop的数据不可用
        _bottom.store(b, butil::memory_order_relaxed);
        return popped;
    }

    // Steal one item from the queue.
    // Returns true on stolen.
    // May run in parallel with push() pop() or another steal().
    bool steal(T* val) {
        size_t t = _top.load(butil::memory_order_acquire);  // 保证能看到pop对buffer的修改
        size_t b = _bottom.load(butil::memory_order_acquire);  // 保证能看到push的buffer的修改
        if (t >= b) {
            // Permit false negative for performance considerations. 可能队列并不空，但是考虑到性能，进行快速判断
            return false;
        }
        do {
            butil::atomic_thread_fence(butil::memory_order_seq_cst);  // 与pop的97行配对， 确保可以看到最新的bottom
            b = _bottom.load(butil::memory_order_acquire); // 确保125行到126行之间有数据push进来能看到push进来的数据
            if (t >= b) {  // 没数据了，失败返回
                return false;
            }
            *val = _buffer[t & (_capacity - 1)];  // 读top的数据
        } while (!_top.compare_exchange_strong(t, t + 1,
                                               butil::memory_order_seq_cst,
                                               butil::memory_order_relaxed));  // 如果top没有改变，说明没有steal和pop和当前线程竞争，直接返回；否则，重新进入循环，seq_cst是和steal，pop的cas配对
        return true;
    }

    size_t volatile_size() const {
        const size_t b = _bottom.load(butil::memory_order_relaxed);
        const size_t t = _top.load(butil::memory_order_relaxed);
        return (b <= t ? 0 : (b - t));
    }

    size_t capacity() const { return _capacity; }

private:
    // Copying a concurrent structure makes no sense.
    DISALLOW_COPY_AND_ASSIGN(WorkStealingQueue);

    butil::atomic<size_t> _bottom;
    size_t _capacity;
    T* _buffer;
    butil::atomic<size_t> BAIDU_CACHELINE_ALIGNMENT _top;  //  对top做内存对齐防止false sharing(伪共享）
};

}  // namespace bthread

#endif  // BTHREAD_WORK_STEALING_QUEUE_H
