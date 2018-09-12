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

final class PoolSubpage<T> implements PoolSubpageMetric {

    final PoolChunk<T> chunk;
    // 当前page在chunk中的id，即 平衡二叉树的 结点 id
    private final int memoryMapIdx;
    // 当前page在chunk.memory的偏移量
    private final int runOffset;
    // page 大小
    private final int pageSize;
    // 通过对每一个二进制位的标记来修改一段内存的占用状态
    private final long[] bitmap;

    PoolSubpage<T> prev;
    PoolSubpage<T> next;

    boolean doNotDestroy;
    // 每一小段的大小，tiny 为 16 倍数， small 为 512 的 2 次幂倍数
    int elemSize;
    // 改 page 包含的 段数
    private int maxNumElems;
    private int bitmapLength;
    // 下一个可用的位置
    private int nextAvail;
    // 可用的 段数
    private int numAvail;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Special constructor that creates a linked list head */
    PoolSubpage(int pageSize) {
        chunk = null;
        memoryMapIdx = -1;
        runOffset = -1;
        elemSize = -1;
        this.pageSize = pageSize;
        bitmap = null;
    }

    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int memoryMapIdx, int runOffset, int pageSize, int elemSize) {
        this.chunk = chunk;
        this.memoryMapIdx = memoryMapIdx;
        this.runOffset = runOffset;
        this.pageSize = pageSize;
        // bitmap 大小 每个段最小 16，因此最多有 pageSize / 16 个 内存段需要 bitmap 维护。一个 long 有 64 位，可以标示 64 个 内存段。
        // 因此 一共需要 pageSize / 16 / 64 个 long 的 bitmap 来管理一个page
        // 默认 pageSize = 8k， 因此 默认 bitmap 有 8 个 long
        bitmap = new long[pageSize >>> 10]; // pageSize / 16 / 64
        init(head, elemSize);
    }

    void init(PoolSubpage<T> head, int elemSize) {
        doNotDestroy = true;
        this.elemSize = elemSize;
        if (elemSize != 0) {
            // 初始化时 计算 一共有多少个内存段和多少个可用内存段 数量为  pageSize / elemSize
            maxNumElems = numAvail = pageSize / elemSize;
            // 从头开始使用，初始化是第一个可用的内存段在 0 位置
            nextAvail = 0;
            // 计算当有 maxNumElems 个内存段时 需要用多少个 long 来描述。 maxNumberElems / 64 需要 long 的个数，即 maxNumElems >>> 6
            bitmapLength = maxNumElems >>> 6;
            // 如果 maxNumElems 不为 64 的整数倍，则需要多一个 long 来描述 超出的段数
            // 比如
            //      maxNumElems = 65 ，65 >>> 6 = 1 ,但是我们需要 1 + 1 = 2 个 long 来描述 所有的内存段
            //      maxNumElems = 3 ，3 >>> 6 = 0 ,但是我们需要 0 + 1 = 1 个 long 来描述 所有的内存段
            //      maxNumElems = 64 ，64 >>> 6 = 1, 此时 只需要 1 个 long 来描述
            if ((maxNumElems & 63) != 0) {
                bitmapLength ++;
            }
            // 初始化标记 所有的内存都可用 设为 0 表明 所有的都可用
            for (int i = 0; i < bitmapLength; i ++) {
                bitmap[i] = 0;
            }
        }
        addToPool(head);
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     */
    long allocate() {
        if (elemSize == 0) {
            return toHandle(0);
        }

        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }
        // 寻找下一个可用的 内存段的 索引值
        final int bitmapIdx = getNextAvail();

        // 0 <= bitmapIdx < pageSize / elemSize
        // bitmapIdx >>> 6 计算 bitmap 数组 下标
        // q 表明 bitmap 数组的第 q 个long 可用来描述 内存段索引为 bitmapIdx 的内存段
        // 这里不用加 1 是因为 数据坐标从 0 开始
        int q = bitmapIdx >>> 6;

        // q 只是找到了哪个 long ，我们必须找到这个 long 的哪个位置能真正的描述 bitmapIdx 索引 的内存段
        // bitmapIdx & 63 将超出 64 的那一部分二进制数抹掉，得到一个小于 64 的数 r
        // r 即 偏移量。比如 bitmapIdx = 66（实际上是第 67 个内存段，index 从 0 开始） ，那么 q = 1 说明 bitmap 的第 2 个 long用来描述改内存段
        // r = bitmapIdx & 63 = bitmapIdx % 64 = 2 即 bitmap 的第 2 个 long 的第 3 个 bit 用来描述 bitmapIdx = 66 的内存段的使用情况
        int r = bitmapIdx & 63;
        // 确保改位置被标记为 可用（ r 偏移位 为 0 ）
        // e.g. 0000000000000000 0000000000000000 0000000000000000 0000000000000100 表明，改 long 的第 3 (r = 2) 个 bit 已经被标记为 已使用
        //      0000000000000000 0000000000000000 0000000000000000 0000000000000100 >>> 2 = 1 就冲突了，不能在标记为 已使用 的位置上申请内存
        assert (bitmap[q] >>> r & 1) == 0;
        // 将 第 q + 1 个 long 的第 r + 1 个位置 标记为 已使用
        // e.g. 本次之前的 bitmap[q] = 0000000000000000 0000000000000000 0000000000000000 0000000000000100 = before
        //      本次 r = 5 ，标记后为   0000000000000000 0000000000000000 0000000000000000 0000000000100000 = current
        //      所以结果为 before | current = 0000000000000000 0000000000000000 0000000000000000 0000000000000100 | 0000000000000000 0000000000000000 0000000000000000 0000000000100000
        //                                 = 0000000000000000 0000000000000000 0000000000000000 0000000000100100
        //      已经有两个位置已被使用
        bitmap[q] |= 1L << r;

        // 可用位置 -1
        // 如果已经没有可用位置，那么就从内存池 移除， 等待 释放后 gc
        if (-- numAvail == 0) {
            removeFromPool();
        }

        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        if (elemSize == 0) {
            return true;
        }
        // bitmap数组坐标为 q 的 long
        int q = bitmapIdx >>> 6;
        // 对应 long 的偏移量
        int r = bitmapIdx & 63;
        // 确保 long 的 r + 1 位置为 1 已经被使用
        assert (bitmap[q] >>> r & 1) != 0;
        // 将 long 的 r + 1 位置 设置为 0，标记成未使用
        // e.g. bitmap[q] = 0000000000000000 0000000000000000 0000000000000000 0001000000000100   r = 2
        //      则 bitmap[q] ^= 1L << r 为 0000000000000000 0000000000000000 0000000000000000 0001000000000100 ^ 100 = 0000000000000000 0000000000000000 0000000000000000 0001000000000000
        //      只改变了 bitmap[q] 的 r + 1 位置的值
        bitmap[q] ^= 1L << r;

        // 标记为下一个可用的 index
        setNextAvail(bitmapIdx);

        // numAvail = 0 说明之前已经从 arena 的 pool 中移除了，现在变回可用，则再次交给 arena 管理
        if (numAvail ++ == 0) {
            addToPool(head);
            return true;
        }
        // 还有可用内存
        if (numAvail != maxNumElems) {
            return true;
        } else {
            // Subpage not in use (numAvail == maxNumElems)
            // prev == next == head 说明 pool 里面只有这一个 subpage ，那么保留；
            if (prev == next) {
                // Do not remove if this subpage is the only one left in the pool.
                return true;
            }
            // pool 里还有其它的 subpage，将这个 subpage 移除，因为已经没有可用的内存了
            // Remove this subpage from the pool if there are other subpages left in the pool.
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    private void addToPool(PoolSubpage<T> head) {
        assert prev == null && next == null;
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    // 寻找下一个可使用的 index
    private int getNextAvail() {
        int nextAvail = this.nextAvail;
        // 如果 nextAvail >= 0 明 nextAvail 指向了下一个可分配的内存段，直接返回 nextAvail 值
        // 每次分配完成，nextAvail 被置为 -1，这时只能通过方法 findNextAvail 重新计算出下一个可分配的内存段位置
        if (nextAvail >= 0) {
            this.nextAvail = -1;
            return nextAvail;
        }
        return findNextAvail();
    }

    private int findNextAvail() {
        final long[] bitmap = this.bitmap;
        // 遍历所有的 long 来找到下一个 可用的 index
        final int bitmapLength = this.bitmapLength;
        for (int i = 0; i < bitmapLength; i ++) {
            long bits = bitmap[i];
            // ～bits != 0 确保 bitmap[i] 这个 long 中还有可用的位置
            if (~bits != 0) {
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    private int findNextAvail0(int i, long bits) {
        final int maxNumElems = this.maxNumElems;
        // long 的基本偏移量 i * 64
        // i = 0 则，最终的 index 应该 加上 0  在 0 - 63 之间
        // i = 1 则，最终的 index 应该 加上 64 在 64 - 127 之间
        // 即 加 i * 64 = i << 6
        final int baseVal = i << 6;

        // bits 共有 64 个位置， 从 0 - 63 依次遍历 找到一个可用位置
        for (int j = 0; j < 64; j ++) {
            // bits & 1 == 0 用来判断 最低位 是否是 为使用状态，如果不是 则 bits 右移 一位
            // 因此 加上外层循环，即用来 判断 第 j + 1 位 是否是可用状态
            if ((bits & 1) == 0) {
                // 如果是可用的，返回 index
                // index 应该由两部分组成：
                //      long 的偏移量 + j 的偏移量
                // long 的偏移 为 i * 64 即 baseVal
                // 由于 long 的偏移量 baseVal >= 64 , j 的偏移量 < 64
                // 因此 baseVal + j = baseVal | j
                int val = baseVal | j;
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            bits >>>= 1;
        }
        return -1;
    }

    private long toHandle(int bitmapIdx) {
        // 2^62 | bitmapIdx * 2^5 |
        return 0x4000000000000000L | (long) bitmapIdx << 32 | memoryMapIdx;
    }

    @Override
    public String toString() {
        final boolean doNotDestroy;
        final int maxNumElems;
        final int numAvail;
        final int elemSize;
        synchronized (chunk.arena) {
            if (!this.doNotDestroy) {
                doNotDestroy = false;
                // Not used for creating the String.
                maxNumElems = numAvail = elemSize = -1;
            } else {
                doNotDestroy = true;
                maxNumElems = this.maxNumElems;
                numAvail = this.numAvail;
                elemSize = this.elemSize;
            }
        }

        if (!doNotDestroy) {
            return "(" + memoryMapIdx + ": not in use)";
        }

        return "(" + memoryMapIdx + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
                ", offset: " + runOffset + ", length: " + pageSize + ", elemSize: " + elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        synchronized (chunk.arena) {
            return maxNumElems;
        }
    }

    @Override
    public int numAvailable() {
        synchronized (chunk.arena) {
            return numAvail;
        }
    }

    @Override
    public int elementSize() {
        synchronized (chunk.arena) {
            return elemSize;
        }
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }
}
