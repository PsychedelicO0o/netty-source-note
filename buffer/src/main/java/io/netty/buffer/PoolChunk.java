/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

/**
 * Description of algorithm for PageRun/PoolSubpage allocation from PoolChunk
 *
 * Notation: The following terms are important to understand the code
 * > page  - a page is the smallest unit of memory chunk that can be allocated
 * > chunk - a chunk is a collection of pages
 * > in this code chunkSize = 2^{maxOrder} * pageSize
 *
 * To begin we allocate a byte array of size = chunkSize
 * Whenever a ByteBuf of given size needs to be created we search for the first position
 * in the byte array that has enough empty space to accommodate the requested size and
 * return a (long) handle that encodes this offset information, (this memory segment is then
 * marked as reserved so it is always used by exactly one ByteBuf and no more)
 *
 * For simplicity all sizes are normalized according to PoolArena#normalizeCapacity method
 * This ensures that when we request for memory segments of size >= pageSize the normalizedCapacity
 * equals the next nearest power of 2
 *
 * To search for the first offset in chunk that has at least requested size available we construct a
 * complete balanced binary tree and store it in an array (just like heaps) - memoryMap
 *
 * The tree looks like this (the size of each node being mentioned in the parenthesis)
 *
 * depth=0        1 node (chunkSize)
 * depth=1        2 nodes (chunkSize/2)
 * ..
 * ..
 * depth=d        2^d nodes (chunkSize/2^d)
 * ..
 * depth=maxOrder 2^maxOrder nodes (chunkSize/2^{maxOrder} = pageSize)
 *
 * depth=maxOrder is the last level and the leafs consist of pages
 *
 * With this tree available searching in chunkArray translates like this:
 * To allocate a memory segment of size chunkSize/2^k we search for the first node (from left) at height k
 * which is unused
 *
 * Algorithm:
 * ----------
 * Encode the tree in memoryMap with the notation
 *   memoryMap[id] = x => in the subtree rooted at id, the first node that is free to be allocated
 *   is at depth x (counted from depth=0) i.e., at depths [depth_of_id, x), there is no node that is free
 *
 *  As we allocate & free nodes, we update values stored in memoryMap so that the property is maintained
 *
 * Initialization -
 *   In the beginning we construct the memoryMap array by storing the depth of a node at each node
 *     i.e., memoryMap[id] = depth_of_id
 *
 * Observations:
 * -------------
 * 1) memoryMap[id] = depth_of_id  => it is free / unallocated
 * 2) memoryMap[id] > depth_of_id  => at least one of its child nodes is allocated, so we cannot allocate it, but
 *                                    some of its children can still be allocated based on their availability
 * 3) memoryMap[id] = maxOrder + 1 => the node is fully allocated & thus none of its children can be allocated, it
 *                                    is thus marked as unusable
 *
 * Algorithm: [allocateNode(d) => we want to find the first node (from left) at height h that can be allocated]
 * ----------
 * 1) start at root (i.e., depth = 0 or id = 1)
 * 2) if memoryMap[1] > d => cannot be allocated from this chunk
 * 3) if left node value <= h; we can allocate from left subtree so move to left and repeat until found
 * 4) else try in right subtree
 *
 * Algorithm: [allocateRun(size)]
 * ----------
 * 1) Compute d = log_2(chunkSize/size)
 * 2) Return allocateNode(d)
 *
 * Algorithm: [allocateSubpage(size)]
 * ----------
 * 1) use allocateNode(maxOrder) to find an empty (i.e., unused) leaf (i.e., page)
 * 2) use this handle to construct the PoolSubpage object or if it already exists just call init(normCapacity)
 *    note that this PoolSubpage object is added to subpagesPool in the PoolArena when we init() it
 *
 * Note:
 * -----
 * In the implementation for improving cache coherence,
 * we store 2 pieces of information (i.e, 2 byte vals) as a short value in memoryMap
 *
 * memoryMap[id]= (depth_of_id, x)
 * where as per convention defined above
 * the second value (i.e, x) indicates that the first node which is free to be allocated is at depth x (from root)
 */
final class PoolChunk<T> implements PoolChunkMetric {

    private static final int INTEGER_SIZE_MINUS_ONE = Integer.SIZE - 1;

    // chunk 所属的 arena
    final PoolArena<T> arena;
    // 实际存储数据的内存块
    final T memory;
    // 是否非池化
    final boolean unpooled;
    final int offset;

    // 分配信息二叉树
    private final byte[] memoryMap;
    // 深度信息二叉树
    private final byte[] depthMap;
    // subpage 节点数组
    private final PoolSubpage<T>[] subpages;
    /** Used to determine if the requested capacity is equal to or greater than pageSize. */
    // 用来辅助判断分配请求是否为 Tiny/Small，即分配subpage  < pageSize 即为 Tiny/Small
    private final int subpageOverflowMask;
    // 页大小，默认 8KB = 8192
    private final int pageSize;
    // 从1开始左移到页大小的位置，即 1 << pageShifts = pageSize 默认13，1 << 13 = 8192
    private final int pageShifts;
    // 二叉树的最大深度 默认 11
    private final int maxOrder;
    // chunk块大小，默认 pageSize * 2^ maxOrder = 8k * 2^11 = 16M
    private final int chunkSize;
    // log2(chunkSize) 默认 log2(16M) = 24
    private final int log2ChunkSize;
    // 可分配 subpage 的最大节点数即 11 层节点数，默认 2048
    private final int maxSubpageAllocs;
    /** Used to mark memory as unusable */
    // 标记节点不可用，最大高度 + 1， 默认12
    private final byte unusable;
    // 可分配字节数
    private int freeBytes;

    PoolChunkList<T> parent;
    PoolChunk<T> prev;
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;
    // 池化内存初始化
    PoolChunk(PoolArena<T> arena, T memory, int pageSize, int maxOrder, int pageShifts, int chunkSize, int offset) {
        unpooled = false;
        this.arena = arena;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.maxOrder = maxOrder;
        this.chunkSize = chunkSize;
        this.offset = offset;
        // 结点完全不可用的标记，maxOrder + 1
        unusable = (byte) (maxOrder + 1);
        log2ChunkSize = log2(chunkSize);
        // ~(pageSize - 1) e.g. pageSize = 8k = 10000000000000， pageSize - 1 = 1111111111111 , ~(pageSize - 1) = 111111....10000000000000
        subpageOverflowMask = ~(pageSize - 1);
        // 初始化时 可用内存 = chunkSize
        freeBytes = chunkSize;

        assert maxOrder < 30 : "maxOrder should be < 30, but is: " + maxOrder;
        // 可申请的 subpage 最大数，即所有的叶子结点都被用于 subpage 分配 ， 共有 1 << maxOrder 个，默认 2048 个
        maxSubpageAllocs = 1 << maxOrder;

        // Generate the memory map.
        // 存储 分配信息 的二叉树，由于叶子结点一共 maxSubpageAllocs 个，所以算上父结点， 一共 maxSubpageAllocs * 2 个
        memoryMap = new byte[maxSubpageAllocs << 1];
        // 存储 深度信息 的二叉树，同样需要 maxSubpageAllocs * 2 个位置来描述所有的结点，包括父结点
        depthMap = new byte[memoryMap.length];

        // 初始化 memoryMap ，depthMap
        int memoryMapIndex = 1;
        // d 为 深度
        for (int d = 0; d <= maxOrder; ++ d) { // move down the tree one level at a time
            // depth 为 深度 为 d 的一层 总共的结点数
            // 依次遍历 将 memoryMap 和 depthMap 结点的值 设置为 本层 深度
            int depth = 1 << d;
            for (int p = 0; p < depth; ++ p) {
                // in each level traverse left to right and set value to the depth of subtree
                memoryMap[memoryMapIndex] = (byte) d;
                depthMap[memoryMapIndex] = (byte) d;
                memoryMapIndex ++;
            }
        }

        // 初始化 subpage 数组，都是空的
        subpages = newSubpageArray(maxSubpageAllocs);
    }

    /** Creates a special chunk that is not pooled. */
    // 特殊构造函数，用于初始化非池化内存
    PoolChunk(PoolArena<T> arena, T memory, int size, int offset) {
        unpooled = true;
        this.arena = arena;
        this.memory = memory;
        this.offset = offset;
        memoryMap = null;
        depthMap = null;
        subpages = null;
        subpageOverflowMask = 0;
        pageSize = 0;
        pageShifts = 0;
        maxOrder = 0;
        unusable = (byte) (maxOrder + 1);
        chunkSize = size;
        log2ChunkSize = log2(chunkSize);
        maxSubpageAllocs = 0;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpage<T>[] newSubpageArray(int size) {
        return new PoolSubpage[size];
    }

    @Override
    public int usage() {
        final int freeBytes;
        // 必须同步，否则 freeBytes 这个值可能被其它线程修改
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }
        // 计算百分比
        return usage(freeBytes);
    }

    private int usage(int freeBytes) {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }
    // 申请内存的入口
    long allocate(int normCapacity) {
        // 用来判断申请的内存是否大于 pageSize
        // subpageOverflowMask = ~(pageSize - 1)，
        //      如果 normCapacity >= pageSize, normCapacity & ~(pageSize - 1)  != 0
        //      如果 normCapacity < pageSize , normCapacity & ~(pageSize - 1) = 0
        //      e.g. pageSize = 8k , ~(pageSize - 1) = 111111....10000000000000
        //                 若 normCapacity = 8k + 1 = 10000000000001 , 则 normCapacity & ~(pageSize - 1) != 0
        //                 若 normCapacity = 8k     = 10000000000000 , 则 normCapacity & ~(pageSize - 1) != 0
        //                 若 normCapacity = 8k - 1 =  1111111111111 , 则 normCapacity & ~(pageSize - 1) = 0
        if ((normCapacity & subpageOverflowMask) != 0) { // >= pageSize
            // 申请 正常大小
            return allocateRun(normCapacity);
        } else {
            // 小于 pageSize 申请 subpage
            return allocateSubpage(normCapacity);
        }
    }

    /**
     * Update method used by allocate
     * This is triggered only when a successor is allocated and all its predecessors
     * need to update their state
     * The minimal depth at which subtree rooted at id has some free space
     *
     * @param id id
     */
    private void updateParentsAlloc(int id) {
        while (id > 1) {
            // 父亲结点
            int parentId = id >>> 1;
            // 当前结点的值
            byte val1 = value(id);
            // 兄弟结点的值
            byte val2 = value(id ^ 1);
            // 取较小的
            byte val = val1 < val2 ? val1 : val2;
            // 设置
            setValue(parentId, val);
            // 向上传播
            id = parentId;
        }
    }

    /**
     * Update method used by free
     * This needs to handle the special case when both children are completely free
     * in which case parent be directly allocated on request of size = child-size * 2
     *
     * @param id id
     */
    private void updateParentsFree(int id) {
        int logChild = depth(id) + 1;
        while (id > 1) {
            int parentId = id >>> 1;
            byte val1 = value(id);
            byte val2 = value(id ^ 1);
            logChild -= 1; // in first iteration equals log, subsequently reduce 1 from logChild as we traverse up

            if (val1 == logChild && val2 == logChild) {
                setValue(parentId, (byte) (logChild - 1));
            } else {
                byte val = val1 < val2 ? val1 : val2;
                setValue(parentId, val);
            }

            id = parentId;
        }
    }

    /**
     * Algorithm to allocate an index in memoryMap when we query for a free node
     * at depth d
     *
     * @param d depth
     * @return index in memoryMap
     */
    // 从深度为 d 层的结点中 寻找第一个能满足 内存需求的 结点的 结点id
    private int allocateNode(int d) {
        //----------- 这是一个中序遍历的非递归实现
        // nodeId ，从第一个开始遍历
        int id = 1;
        int initial = - (1 << d); // has last d bits = 0 and rest all = 1
        // nodeId 对应的 memoryMap 的值，存放的深度
        byte val = value(id);
        // 如果val > d 说明不够申请，因此找不到可用 node 结点
        if (val > d) { // unusable
            return -1;
        }

        // 如果 val = d 说明找到了，不用进循环
        // 如果 val < d ,说明当前结点能满足申请，可以在当前结点下的子结点申请内存
        //  如果是 d 层以上的结点，(id & initial) = 0 ，如果是 d 层的结点， 这个值不为 0
        //  这样做是为了快速的定位到 d - 1 层，观察 d - 1 层的 左右孩子结点就可以了
        while (val < d || (id & initial) == 0) { // id & initial == 1 << d for all ids at depth d, for < d it is 0
            // 观察左孩子结点
            id <<= 1;
            val = value(id);
            // val > d 左孩子结点 不满足
            if (val > d) {
                // 观察右孩子结点，右孩子结点肯定满足
                id ^= 1;
                val = value(id);
                // 如果 val = d，那么说明找到了，下次循环进不来
                // val < d 那么说明右孩子结点的孩子结点能满足，继续循环
            }
            // 如果 val = d 那么说明找到了，下次循环进不来
            // 如果 val < d 那么说明左孩子结点的孩子结点能满足，继续循环
        }
        //----------- 中序遍历 结束
        byte value = value(id);
        // 确保 结点的深度为 d，其在第 d 层
        assert value == d && (id & initial) == 1 << d : String.format("val = %d, id & initial = %d, d = %d",
                value, id & initial, d);
        // 标记为已使用
        setValue(id, unusable); // mark as unusable
        // 更新所有父结点的 memoryMap 的值，更新方式为 取两个孩子结点 memoryMap 的值的较小的一个
        updateParentsAlloc(id);
        return id;
    }

    /**
     * Allocate a run of pages (>=1)
     *
     * @param normCapacity normalized capacity
     * @return index in memoryMap
     */
    // normCapacity >= pageSize 至少占用 1 个page
    // 返回 page 的 nodeid 即 memoryMap 的索引值
    private long allocateRun(int normCapacity) {
        // 计算 满足 normCapacity 大小的内存 应该去二叉树多深的深度 去申请, normCapacity 为 pageSize 的 2次幂倍
        // 假设 normCapacity 是 pageSize 的 2^n 倍，即 normCapacity = 2^n * pageSize
        //      由于 pageSize = 1 << pageShirfts，
        //      因此 normCapacity = 2^n * 1 << pageShirfts
        //          log2(normCapacity) = n + pageShirfts ,
        //          d = maxOrder - (n + pageShirfts - pageShirfts) = maxOrder - n
        //      通过 memoryMap 我们知道 大小为 normCapacity = 2^n * pageSize 的结点是在 最后一层结点的 n 层之上描述
        //      因此 maxOrder - n 就知道了哪层能刚好满足需求,恰好是 d
        int d = maxOrder - (log2(normCapacity) - pageShifts);
        int id = allocateNode(d);
        // id < 0 表明 没有找到
        if (id < 0) {
            return id;
        }
        // 修改可用的内存数
        freeBytes -= runLength(id);
        return id;
    }

    /**
     * Create/ initialize a new PoolSubpage of normCapacity
     * Any PoolSubpage created/ initialized here is added to subpage pool in the PoolArena that owns this PoolChunk
     *
     * @param normCapacity normalized capacity
     * @return index in memoryMap
     */
    private long allocateSubpage(int normCapacity) {
        // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
        // This is need as we may add it back and so alter the linked-list structure.
        PoolSubpage<T> head = arena.findSubpagePoolHead(normCapacity);
        synchronized (head) {
            int d = maxOrder; // subpages are only be allocated from pages i.e., leaves
            int id = allocateNode(d);
            if (id < 0) {
                return id;
            }

            final PoolSubpage<T>[] subpages = this.subpages;
            final int pageSize = this.pageSize;

            freeBytes -= pageSize;

            int subpageIdx = subpageIdx(id);
            PoolSubpage<T> subpage = subpages[subpageIdx];
            if (subpage == null) {
                subpage = new PoolSubpage<T>(head, this, id, runOffset(id), pageSize, normCapacity);
                subpages[subpageIdx] = subpage;
            } else {
                subpage.init(head, normCapacity);
            }
            return subpage.allocate();
        }
    }

    /**
     * Free a subpage or a run of pages
     * When a subpage is freed from PoolSubpage, it might be added back to subpage pool of the owning PoolArena
     * If the subpage pool in PoolArena has at least one other PoolSubpage of given elemSize, we can
     * completely free the owning Page so it is available for subsequent allocations
     *
     * @param handle handle to free
     */
    void free(long handle) {
        int memoryMapIdx = memoryMapIdx(handle);
        int bitmapIdx = bitmapIdx(handle);

        if (bitmapIdx != 0) { // free a subpage
            PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
            assert subpage != null && subpage.doNotDestroy;

            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
            // This is need as we may add it back and so alter the linked-list structure.
            PoolSubpage<T> head = arena.findSubpagePoolHead(subpage.elemSize);
            synchronized (head) {
                if (subpage.free(head, bitmapIdx & 0x3FFFFFFF)) {
                    return;
                }
            }
        }
        freeBytes += runLength(memoryMapIdx);
        setValue(memoryMapIdx, depth(memoryMapIdx));
        updateParentsFree(memoryMapIdx);
    }
    // normal 大小的 Buf 初始化
    void initBuf(PooledByteBuf<T> buf, long handle, int reqCapacity) {
        // 获取申请的内存块在chunk里的 索引 位置 即 第几个 page
        int memoryMapIdx = memoryMapIdx(handle);
        // 获取申请的内存块在 page 里的 索引 位置 即 subpage
        int bitmapIdx = bitmapIdx(handle);
        // bitmapIdx == 0 说明 申请的是 normal 块
        if (bitmapIdx == 0) {
            // nodeId 对应的 memoryMap 的值，存放的可用深度
            byte val = value(memoryMapIdx);
            // val == unusable == maxOrder + 1 说明已经全部使用完了
            assert val == unusable : String.valueOf(val);
            // 初始化 buf 写入 大小 偏移 最大容量 等
            buf.init(this, handle, runOffset(memoryMapIdx) + offset, reqCapacity, runLength(memoryMapIdx),
                     arena.parent.threadCache());
        } else {
            // 如果 bitmapIdx ！= 0 说明是 subpage
            initBufWithSubpage(buf, handle, bitmapIdx, reqCapacity);
        }
    }
    // tiny / small 大小的 Buf 初始化
    void initBufWithSubpage(PooledByteBuf<T> buf, long handle, int reqCapacity) {
        initBufWithSubpage(buf, handle, bitmapIdx(handle), reqCapacity);
    }
    //
    private void initBufWithSubpage(PooledByteBuf<T> buf, long handle, int bitmapIdx, int reqCapacity) {
        assert bitmapIdx != 0;
        // page index
        int memoryMapIdx = memoryMapIdx(handle);
        // subpageIdx(memoryMapIdx) 根据 page index 得出 subpage 在 memoryMap 最后一层的 偏移索引
        // 有了偏移索引 拿出对应的 subpage
        PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
        assert subpage.doNotDestroy;
        assert reqCapacity <= subpage.elemSize;
        // 初始化 buf 写入相关信息
        buf.init(
            this, handle,
            runOffset(memoryMapIdx) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize + offset,
                reqCapacity, subpage.elemSize, arena.parent.threadCache());
    }

    private byte value(int id) {
        return memoryMap[id];
    }

    private void setValue(int id, byte val) {
        memoryMap[id] = val;
    }

    private byte depth(int id) {
        return depthMap[id];
    }

    private static int log2(int val) {
        // compute the (0-based, with lsb = 0) position of highest set bit i.e, log2
        return INTEGER_SIZE_MINUS_ONE - Integer.numberOfLeadingZeros(val);
    }
    // 得到节点对应可分配的字节，1号节点为16MB-ChunkSize，2048节点为8KB-PageSize
    private int runLength(int id) {
        // represents the size in #bytes supported by node 'id' in the tree
        return 1 << log2ChunkSize - depth(id);
    }
    // 得到节点在chunk底层的字节数组中的偏移量
    // 2048-0, 2049-8K，2050-16K
    private int runOffset(int id) {
        // represents the 0-based offset in #bytes from start of the byte-array chunk
        int shift = id ^ 1 << depth(id);
        return shift * runLength(id);
    }
    // 得到最后一层节点的偏移索引，= id - 2^maxOrder
    private int subpageIdx(int memoryMapIdx) {
        return memoryMapIdx ^ maxSubpageAllocs; // remove highest set bit, to get offset
    }
    // 低 32 位 为 memoryMap 的索引值
    private static int memoryMapIdx(long handle) {
        return (int) handle;
    }
    // 高 32 为 为 用来标记 subpage 使用情况的 bitmap 的索引值
    private static int bitmapIdx(long handle) {
        return (int) (handle >>> Integer.SIZE);
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public int freeBytes() {
        synchronized (arena) {
            return freeBytes;
        }
    }

    @Override
    public String toString() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }

        return new StringBuilder()
                .append("Chunk(")
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append(": ")
                .append(usage(freeBytes))
                .append("%, ")
                .append(chunkSize - freeBytes)
                .append('/')
                .append(chunkSize)
                .append(')')
                .toString();
    }

    void destroy() {
        arena.destroyChunk(this);
    }
}
