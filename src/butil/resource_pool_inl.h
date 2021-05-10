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

// Date: Sun Jul 13 15:04:18 CST 2014

#ifndef BUTIL_RESOURCE_POOL_INL_H
#define BUTIL_RESOURCE_POOL_INL_H

#include <iostream>                      // std::ostream
#include <pthread.h>                     // pthread_mutex_t
#include <algorithm>                     // std::max, std::min
#include "butil/atomicops.h"              // butil::atomic
#include "butil/macros.h"                 // BAIDU_CACHELINE_ALIGNMENT
#include "butil/scoped_lock.h"            // BAIDU_SCOPED_LOCK
#include "butil/thread_local.h"           // thread_atexit
#include <vector>

#ifdef BUTIL_RESOURCE_POOL_NEED_FREE_ITEM_NUM
#define BAIDU_RESOURCE_POOL_FREE_ITEM_NUM_ADD1                \
    (_global_nfree.fetch_add(1, butil::memory_order_relaxed))
#define BAIDU_RESOURCE_POOL_FREE_ITEM_NUM_SUB1                \
    (_global_nfree.fetch_sub(1, butil::memory_order_relaxed))
#else
#define BAIDU_RESOURCE_POOL_FREE_ITEM_NUM_ADD1
#define BAIDU_RESOURCE_POOL_FREE_ITEM_NUM_SUB1
#endif

namespace butil {

// ResourceId是resource pool中某个资源的唯一标识，所有的资源获取和归还都是基于ResouceId的
template <typename T>
struct ResourceId {
    uint64_t value;

    operator uint64_t() const {
        return value;
    }

    template <typename T2>
    ResourceId<T2> cast() const {
        ResourceId<T2> id = { value };
        return id;
    }
};

template <typename T, size_t NITEM>
struct ResourcePoolFreeChunk {
    size_t nfree;  // 空闲的资源个数
    ResourceId<T> ids[NITEM];  // ResourceId数组
};
// for gcc 3.4.5
template <typename T>
struct ResourcePoolFreeChunk<T, 0> {
    size_t nfree;
    ResourceId<T> ids[0]; // GNU c 中的用法，长度为0的数组，用于变长buffer
};

struct ResourcePoolInfo {
    size_t local_pool_num;
    size_t block_group_num;
    size_t block_num;
    size_t item_num;
    size_t block_item_num;
    size_t free_chunk_item_num;
    size_t total_size;
#ifdef BUTIL_RESOURCE_POOL_NEED_FREE_ITEM_NUM
    size_t free_item_num;
#endif
};

static const size_t RP_MAX_BLOCK_NGROUP = 65536;
static const size_t RP_GROUP_NBLOCK_NBIT = 16;
static const size_t RP_GROUP_NBLOCK = (1UL << RP_GROUP_NBLOCK_NBIT);
static const size_t RP_INITIAL_FREE_LIST_SIZE = 1024;

// 资源池中一个block中的item数
template <typename T>
class ResourcePoolBlockItemNum {
    static const size_t N1 = ResourcePoolBlockMaxSize<T>::value / sizeof(T);
    static const size_t N2 = (N1 < 1 ? 1 : N1);
public:
    static const size_t value = (N2 > ResourcePoolBlockMaxItem<T>::value ?
                                 ResourcePoolBlockMaxItem<T>::value : N2);
};


template <typename T>
class BAIDU_CACHELINE_ALIGNMENT ResourcePool {
public:
    static const size_t BLOCK_NITEM = ResourcePoolBlockItemNum<T>::value;
    static const size_t FREE_CHUNK_NITEM = BLOCK_NITEM;

    // Free identifiers are batched in a FreeChunk before they're added to
    // global list(_free_chunks).
    typedef ResourcePoolFreeChunk<T, FREE_CHUNK_NITEM>      FreeChunk;  // 固定大小的freechunk
    typedef ResourcePoolFreeChunk<T, 0> DynamicFreeChunk;  // 利用柔性数组实现的变长chunk

    // When a thread needs memory, it allocates a Block. To improve locality,
    // items in the Block are only used by the thread.
    // To support cache-aligned objects, align Block.items by cacheline.
    // Resource pool在内存分配上是按块来的，BLock受BlockGroup管理
    struct BAIDU_CACHELINE_ALIGNMENT Block {
        char items[sizeof(T) * BLOCK_NITEM]; // 提前分配好内存
        size_t nitem; // 当前Block中已建立的item的数量，初始化为0

        Block() : nitem(0) {}
    };

    // A Resource addresses at most RP_MAX_BLOCK_NGROUP BlockGroups,
    // each BlockGroup addresses at most RP_GROUP_NBLOCK blocks. So a
    // resource addresses at most RP_MAX_BLOCK_NGROUP * RP_GROUP_NBLOCK Blocks.
    struct BlockGroup {
        butil::atomic<size_t> nblock;  // Block数量，初始化为0
        butil::atomic<Block*> blocks[RP_GROUP_NBLOCK];  // Block指针数组，提前分配内存，构造函数中所有指针初始化为NULL

        BlockGroup() : nblock(0) {
            // We fetch_add nblock in add_block() before setting the entry,
            // thus address_resource() may sees the unset entry. Initialize
            // all entries to NULL makes such address_resource() return NULL.
            memset(blocks, 0, sizeof(butil::atomic<Block*>) * RP_GROUP_NBLOCK);
        }
    };


    // Each thread has an instance of this class.
    class BAIDU_CACHELINE_ALIGNMENT LocalPool {
    public:
        explicit LocalPool(ResourcePool* pool)
            : _pool(pool)
            , _cur_block(NULL)
            , _cur_block_index(0) {
            _cur_free.nfree = 0;
        }

        ~LocalPool() {
            // Add to global _free_chunks if there're some free resources
            // 线程退出的时候如果有空闲的资源，就把它放到全局中去
            if (_cur_free.nfree) {
                _pool->push_free_chunk(_cur_free);
            }

            _pool->clear_from_destructor_of_local_pool();
        }

        static void delete_local_pool(void* arg) {
            delete(LocalPool*)arg;
        }

        // We need following macro to construct T with different CTOR_ARGS
        // which may include parenthesis because when T is POD, "new T()"
        // and "new T" are different: former one sets all fields to 0 which
        // we don't want.
        // POD，Plain Old Data，基本数据类型、指针、union、数组、trivial构造函数的struct或者
        // class都属于这类数据，这类数据属于C++中与C相兼容的数据，可以按照C的方式来处理
        // （运算、拷贝等），这里是想要调用new T而不是new T()来避免不必要的memset，节省开销
        //
#define BAIDU_RESOURCE_POOL_GET(CTOR_ARGS)                              \
        /* Fetch local free id, 本地有空闲资源，直接从block中返回 */                                       \
        if (_cur_free.nfree) {                                          \
            const ResourceId<T> free_id = _cur_free.ids[--_cur_free.nfree]; \
            *id = free_id;                                              \
            BAIDU_RESOURCE_POOL_FREE_ITEM_NUM_SUB1;                   \
            return unsafe_address_resource(free_id);                    \
        }                                                               \
        /* Fetch a FreeChunk from global.                               \
           TODO: Popping from _free needs to copy a FreeChunk which is  \
           costly, but hardly impacts amortized performance.            \
           从全局的free_chunk中取空闲资源                     */         \
        if (_pool->pop_free_chunk(_cur_free)) {                         \
            --_cur_free.nfree;                                          \
            const ResourceId<T> free_id =  _cur_free.ids[_cur_free.nfree]; \
            *id = free_id;                                              \
            BAIDU_RESOURCE_POOL_FREE_ITEM_NUM_SUB1;                   \
            return unsafe_address_resource(free_id);                    \
        }                                                               \
        /* Fetch memory from local block  在本地block新建资源*/          \
        if (_cur_block && _cur_block->nitem < BLOCK_NITEM) {            \
            id->value = _cur_block_index * BLOCK_NITEM + _cur_block->nitem; \
            /* 在指定内存空间上新建对象，而不分配内存 */                        \
            T* p = new ((T*)_cur_block->items + _cur_block->nitem) T CTOR_ARGS; \
            if (!ResourcePoolValidator<T>::validate(p)) {               \
                p->~T();                                                \
                return NULL;                                            \
            }                                                           \
            ++_cur_block->nitem;                                        \
            return p;                                                   \
        }                                                               \
        /* Fetch a Block from global */                                 \
        /* 如果_cur_block没初始化或者已经满了，                           \
        * 则新建一个block把指针赋给_cur_block，在block中新建对象 */        \
        _cur_block = add_block(&_cur_block_index);                      \
        if (_cur_block != NULL) {                                       \
            id->value = _cur_block_index * BLOCK_NITEM + _cur_block->nitem; \
            T* p = new ((T*)_cur_block->items + _cur_block->nitem) T CTOR_ARGS; \
            if (!ResourcePoolValidator<T>::validate(p)) {               \
                p->~T();                                                \
                return NULL;                                            \
            }                                                           \
            ++_cur_block->nitem;                                        \
            return p;                                                   \
        }                                                               \
        return NULL;                                                    \


        inline T* get(ResourceId<T>* id) {
            BAIDU_RESOURCE_POOL_GET();
        }

        template <typename A1>
        inline T* get(ResourceId<T>* id, const A1& a1) {
            BAIDU_RESOURCE_POOL_GET((a1));
        }

        template <typename A1, typename A2>
        inline T* get(ResourceId<T>* id, const A1& a1, const A2& a2) {
            BAIDU_RESOURCE_POOL_GET((a1, a2));
        }

#undef BAIDU_RESOURCE_POOL_GET

        // 归还资源
        inline int return_resource(ResourceId<T> id) {
            // Return to local free list 优先还到本地的freechunk中去
            if (_cur_free.nfree < ResourcePool::free_chunk_nitem()) {
                _cur_free.ids[_cur_free.nfree++] = id;
                BAIDU_RESOURCE_POOL_FREE_ITEM_NUM_ADD1;
                return 0;
            }
            // Local free list is full, return it to global.
            // For copying issue, check comment in upper get()
            // 本地freechunk满了，就把这个chunk放到全局_free_chunks中去
            // 然后在这个空闲的资源id放到本地的_cur_free中
            if (_pool->push_free_chunk(_cur_free)) {
                _cur_free.nfree = 1;
                _cur_free.ids[0] = id;
                BAIDU_RESOURCE_POOL_FREE_ITEM_NUM_ADD1;
                return 0;
            }
            return -1;
        }

    private:
        ResourcePool* _pool;
        Block* _cur_block;
        size_t _cur_block_index;
        FreeChunk _cur_free;
    };

    static inline T* unsafe_address_resource(ResourceId<T> id) {
        const size_t block_index = id.value / BLOCK_NITEM;
        return (T*)(_block_groups[(block_index >> RP_GROUP_NBLOCK_NBIT)]
                    .load(butil::memory_order_consume)
                    ->blocks[(block_index & (RP_GROUP_NBLOCK - 1))]
                    .load(butil::memory_order_consume)->items) +
               id.value - block_index * BLOCK_NITEM;
    }

    static inline T* address_resource(ResourceId<T> id) {
        const size_t block_index = id.value / BLOCK_NITEM;
        const size_t group_index = (block_index >> RP_GROUP_NBLOCK_NBIT);
        if (__builtin_expect(group_index < RP_MAX_BLOCK_NGROUP, 1)) {
            BlockGroup* bg =
                _block_groups[group_index].load(butil::memory_order_consume);
            if (__builtin_expect(bg != NULL, 1)) {
                Block* b = bg->blocks[block_index & (RP_GROUP_NBLOCK - 1)]
                           .load(butil::memory_order_consume);
                if (__builtin_expect(b != NULL, 1)) {
                    const size_t offset = id.value - block_index * BLOCK_NITEM;
                    if (__builtin_expect(offset < b->nitem, 1)) {
                        return (T*)b->items + offset;
                    }
                }
            }
        }

        return NULL;
    }

    inline T* get_resource(ResourceId<T>* id) {
        LocalPool* lp = get_or_new_local_pool();
        if (__builtin_expect(lp != NULL, 1)) {
            return lp->get(id);
        }
        return NULL;
    }

    template <typename A1>
    inline T* get_resource(ResourceId<T>* id, const A1& arg1) {
        LocalPool* lp = get_or_new_local_pool();
        if (__builtin_expect(lp != NULL, 1)) {
            return lp->get(id, arg1);
        }
        return NULL;
    }

    template <typename A1, typename A2>
    inline T* get_resource(ResourceId<T>* id, const A1& arg1, const A2& arg2) {
        LocalPool* lp = get_or_new_local_pool();
        if (__builtin_expect(lp != NULL, 1)) {
            return lp->get(id, arg1, arg2);
        }
        return NULL;
    }

    inline int return_resource(ResourceId<T> id) {
        LocalPool* lp = get_or_new_local_pool();
        if (__builtin_expect(lp != NULL, 1)) {
            return lp->return_resource(id);
        }
        return -1;
    }

    void clear_resources() {
        LocalPool* lp = _local_pool;
        if (lp) {
            _local_pool = NULL;
            butil::thread_atexit_cancel(LocalPool::delete_local_pool, lp);
            delete lp;
        }
    }

    static inline size_t free_chunk_nitem() {
        const size_t n = ResourcePoolFreeChunkMaxItem<T>::value();
        return n < FREE_CHUNK_NITEM ? n : FREE_CHUNK_NITEM;
    }

    // Number of all allocated objects, including being used and free.
    ResourcePoolInfo describe_resources() const {
        ResourcePoolInfo info;
        info.local_pool_num = _nlocal.load(butil::memory_order_relaxed);
        info.block_group_num = _ngroup.load(butil::memory_order_acquire);
        info.block_num = 0;
        info.item_num = 0;
        info.free_chunk_item_num = free_chunk_nitem();
        info.block_item_num = BLOCK_NITEM;
#ifdef BUTIL_RESOURCE_POOL_NEED_FREE_ITEM_NUM
        info.free_item_num = _global_nfree.load(butil::memory_order_relaxed);
#endif

        for (size_t i = 0; i < info.block_group_num; ++i) {
            BlockGroup* bg = _block_groups[i].load(butil::memory_order_consume);
            if (NULL == bg) {
                break;
            }
            size_t nblock = std::min(bg->nblock.load(butil::memory_order_relaxed),
                                     RP_GROUP_NBLOCK);
            info.block_num += nblock;
            for (size_t j = 0; j < nblock; ++j) {
                Block* b = bg->blocks[j].load(butil::memory_order_consume);
                if (NULL != b) {
                    info.item_num += b->nitem;
                }
            }
        }
        info.total_size = info.block_num * info.block_item_num * sizeof(T);
        return info;
    }

    // 使用release-consume语义来保证某线程的新建对其他线程读取的可见性
    static inline ResourcePool* singleton() {
        ResourcePool* p = _singleton.load(butil::memory_order_consume);
        if (p) {
            return p;
        }
        pthread_mutex_lock(&_singleton_mutex);
        p = _singleton.load(butil::memory_order_consume);
        if (!p) {
            p = new ResourcePool();
            _singleton.store(p, butil::memory_order_release);
        }
        pthread_mutex_unlock(&_singleton_mutex);
        return p;
    }

private:
    ResourcePool() {
        _free_chunks.reserve(RP_INITIAL_FREE_LIST_SIZE);
        pthread_mutex_init(&_free_chunks_mutex, NULL);
    }

    ~ResourcePool() {
        pthread_mutex_destroy(&_free_chunks_mutex);
    }

    // Create a Block and append it to right-most BlockGroup.
    static Block* add_block(size_t* index) {
        Block* const new_block = new(std::nothrow) Block;
        if (NULL == new_block) {
            return NULL;
        }

        size_t ngroup;
        do {
            ngroup = _ngroup.load(butil::memory_order_acquire);
            if (ngroup >= 1) {
                BlockGroup* const g =
                    _block_groups[ngroup - 1].load(butil::memory_order_consume);
                const size_t block_index =
                    g->nblock.fetch_add(1, butil::memory_order_relaxed);
                if (block_index < RP_GROUP_NBLOCK) {
                    g->blocks[block_index].store(
                        new_block, butil::memory_order_release);
                    *index = (ngroup - 1) * RP_GROUP_NBLOCK + block_index;
                    return new_block;
                }
                g->nblock.fetch_sub(1, butil::memory_order_relaxed);
            }
        } while (add_block_group(ngroup));

        // Fail to add_block_group.
        delete new_block;
        return NULL;
    }

    // Create a BlockGroup and append it to _block_groups.
    // Shall be called infrequently because a BlockGroup is pretty big.
    static bool add_block_group(size_t old_ngroup) {
        BlockGroup* bg = NULL;
        BAIDU_SCOPED_LOCK(_block_group_mutex);
        const size_t ngroup = _ngroup.load(butil::memory_order_acquire);
        if (ngroup != old_ngroup) {
            // Other thread got lock and added group before this thread.
            return true;
        }
        if (ngroup < RP_MAX_BLOCK_NGROUP) {
            bg = new(std::nothrow) BlockGroup;
            if (NULL != bg) {
                // Release fence is paired with consume fence in address() and
                // add_block() to avoid un-constructed bg to be seen by other
                // threads.
                _block_groups[ngroup].store(bg, butil::memory_order_release);
                _ngroup.store(ngroup + 1, butil::memory_order_release);
            }
        }
        return bg != NULL;
    }

    inline LocalPool* get_or_new_local_pool() {
        LocalPool* lp = _local_pool;
        // 判断_local_pool是否已经有了，有就直接返回，没有就新建
        if (lp != NULL) {
            return lp;
        }
        lp = new(std::nothrow) LocalPool(this);
        if (NULL == lp) {
            return NULL;
        }
        BAIDU_SCOPED_LOCK(_change_thread_mutex); //avoid race with clear()
        _local_pool = lp;
        butil::thread_atexit(LocalPool::delete_local_pool, lp);  // 注册线程退出函数，pthread退出后删除_local_pool
        _nlocal.fetch_add(1, butil::memory_order_relaxed);
        return lp;
    }

    void clear_from_destructor_of_local_pool() {
        // Remove tls
        _local_pool = NULL;

        if (_nlocal.fetch_sub(1, butil::memory_order_relaxed) != 1) {
            return;
        }

        // Can't delete global even if all threads(called ResourcePool
        // functions) quit because the memory may still be referenced by
        // other threads. But we need to validate that all memory can
        // be deallocated correctly in tests, so wrap the function with
        // a macro which is only defined in unittests.
#ifdef BAIDU_CLEAR_RESOURCE_POOL_AFTER_ALL_THREADS_QUIT
        BAIDU_SCOPED_LOCK(_change_thread_mutex);  // including acquire fence.
        // Do nothing if there're active threads.
        if (_nlocal.load(butil::memory_order_relaxed) != 0) {
            return;
        }
        // All threads exited and we're holding _change_thread_mutex to avoid
        // racing with new threads calling get_resource().

        // Clear global free list.
        FreeChunk dummy;
        while (pop_free_chunk(dummy));

        // Delete all memory
        const size_t ngroup = _ngroup.exchange(0, butil::memory_order_relaxed);
        for (size_t i = 0; i < ngroup; ++i) {
            BlockGroup* bg = _block_groups[i].load(butil::memory_order_relaxed);
            if (NULL == bg) {
                break;
            }
            size_t nblock = std::min(bg->nblock.load(butil::memory_order_relaxed),
                                     RP_GROUP_NBLOCK);
            for (size_t j = 0; j < nblock; ++j) {
                Block* b = bg->blocks[j].load(butil::memory_order_relaxed);
                if (NULL == b) {
                    continue;
                }
                for (size_t k = 0; k < b->nitem; ++k) {
                    T* const objs = (T*)b->items;
                    objs[k].~T();
                }
                delete b;
            }
            delete bg;
        }

        memset(_block_groups, 0, sizeof(BlockGroup*) * RP_MAX_BLOCK_NGROUP);
#endif
    }

private:
    bool pop_free_chunk(FreeChunk& c) {
        // Critical for the case that most return_object are called in
        // different threads of get_object.
        if (_free_chunks.empty()) {
            return false;
        }
        pthread_mutex_lock(&_free_chunks_mutex);
        if (_free_chunks.empty()) {
            pthread_mutex_unlock(&_free_chunks_mutex);
            return false;
        }
        DynamicFreeChunk* p = _free_chunks.back();
        _free_chunks.pop_back();
        pthread_mutex_unlock(&_free_chunks_mutex);
        c.nfree = p->nfree;
        memcpy(c.ids, p->ids, sizeof(*p->ids) * p->nfree);
        free(p);
        return true;
    }

    bool push_free_chunk(const FreeChunk& c) {
        DynamicFreeChunk* p = (DynamicFreeChunk*)malloc(
            offsetof(DynamicFreeChunk, ids) + sizeof(*c.ids) * c.nfree);  // 利用DynamicFreeChunk柔性数组的特性，根据c的大小分配内存
        if (!p) {
            return false;
        }
        p->nfree = c.nfree;
        memcpy(p->ids, c.ids, sizeof(*c.ids) * c.nfree);
        pthread_mutex_lock(&_free_chunks_mutex);
        _free_chunks.push_back(p);
        pthread_mutex_unlock(&_free_chunks_mutex);
        return true;
    }

    static butil::static_atomic<ResourcePool*> _singleton;
    static pthread_mutex_t _singleton_mutex;
    static BAIDU_THREAD_LOCAL LocalPool* _local_pool;
    static butil::static_atomic<long> _nlocal;  // 表明T来兴的resource pool的local pool数量
    static butil::static_atomic<size_t> _ngroup;
    static pthread_mutex_t _block_group_mutex;
    static pthread_mutex_t _change_thread_mutex;
    static butil::static_atomic<BlockGroup*> _block_groups[RP_MAX_BLOCK_NGROUP];

    std::vector<DynamicFreeChunk*> _free_chunks;
    pthread_mutex_t _free_chunks_mutex;

#ifdef BUTIL_RESOURCE_POOL_NEED_FREE_ITEM_NUM
    static butil::static_atomic<size_t> _global_nfree;
#endif
};

// Declare template static variables:

template <typename T>
const size_t ResourcePool<T>::FREE_CHUNK_NITEM;

template <typename T>
BAIDU_THREAD_LOCAL typename ResourcePool<T>::LocalPool*
ResourcePool<T>::_local_pool = NULL;

template <typename T>
butil::static_atomic<ResourcePool<T>*> ResourcePool<T>::_singleton =
    BUTIL_STATIC_ATOMIC_INIT(NULL);

template <typename T>
pthread_mutex_t ResourcePool<T>::_singleton_mutex = PTHREAD_MUTEX_INITIALIZER;

template <typename T>
butil::static_atomic<long> ResourcePool<T>::_nlocal = BUTIL_STATIC_ATOMIC_INIT(0);

template <typename T>
butil::static_atomic<size_t> ResourcePool<T>::_ngroup = BUTIL_STATIC_ATOMIC_INIT(0);

template <typename T>
pthread_mutex_t ResourcePool<T>::_block_group_mutex = PTHREAD_MUTEX_INITIALIZER;

template <typename T>
pthread_mutex_t ResourcePool<T>::_change_thread_mutex =
    PTHREAD_MUTEX_INITIALIZER;

template <typename T>
butil::static_atomic<typename ResourcePool<T>::BlockGroup*>
ResourcePool<T>::_block_groups[RP_MAX_BLOCK_NGROUP] = {};

#ifdef BUTIL_RESOURCE_POOL_NEED_FREE_ITEM_NUM
template <typename T>
butil::static_atomic<size_t> ResourcePool<T>::_global_nfree = BUTIL_STATIC_ATOMIC_INIT(0);
#endif

template <typename T>
inline bool operator==(ResourceId<T> id1, ResourceId<T> id2) {
    return id1.value == id2.value;
}

template <typename T>
inline bool operator!=(ResourceId<T> id1, ResourceId<T> id2) {
    return id1.value != id2.value;
}

// Disable comparisons between different typed ResourceId
template <typename T1, typename T2>
bool operator==(ResourceId<T1> id1, ResourceId<T2> id2);

template <typename T1, typename T2>
bool operator!=(ResourceId<T1> id1, ResourceId<T2> id2);

inline std::ostream& operator<<(std::ostream& os,
                                ResourcePoolInfo const& info) {
    return os << "local_pool_num: " << info.local_pool_num
              << "\nblock_group_num: " << info.block_group_num
              << "\nblock_num: " << info.block_num
              << "\nitem_num: " << info.item_num
              << "\nblock_item_num: " << info.block_item_num
              << "\nfree_chunk_item_num: " << info.free_chunk_item_num
              << "\ntotal_size: " << info.total_size;
#ifdef BUTIL_RESOURCE_POOL_NEED_FREE_ITEM_NUM
              << "\nfree_num: " << info.free_item_num
#endif
           ;
}

}  // namespace butil

#endif  // BUTIL_RESOURCE_POOL_INL_H
