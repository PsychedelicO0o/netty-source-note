/*
 * Copyright 2017 The Netty Project
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

import io.netty.util.internal.PlatformDependent;
import org.junit.Test;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractByteBufAllocatorTest<T extends AbstractByteBufAllocator> extends ByteBufAllocatorTest {

    @Override
    protected abstract T newAllocator(boolean preferDirect);

    protected abstract T newUnpooledAllocator();

    @Override
    protected boolean isDirectExpected(boolean preferDirect) {
        return preferDirect && PlatformDependent.hasUnsafe();
    }

    @Override
    protected final int defaultMaxCapacity() {
        return AbstractByteBufAllocator.DEFAULT_MAX_CAPACITY;
    }

    @Override
    protected final int defaultMaxComponents() {
        return AbstractByteBufAllocator.DEFAULT_MAX_COMPONENTS;
    }

    @Test
    public void testUnsafeHeapBufferAndUnsafeDirectBuffer() {
        T allocator = newUnpooledAllocator();
        ByteBuf directBuffer = allocator.directBuffer();
        assertInstanceOf(directBuffer,
                PlatformDependent.hasUnsafe() ? UnpooledUnsafeDirectByteBuf.class : UnpooledDirectByteBuf.class);
        directBuffer.release();

        ByteBuf heapBuffer = allocator.heapBuffer();
        assertInstanceOf(heapBuffer,
                PlatformDependent.hasUnsafe() ? UnpooledUnsafeHeapByteBuf.class : UnpooledHeapByteBuf.class);
        heapBuffer.release();
    }

    protected static void assertInstanceOf(ByteBuf buffer, Class<? extends ByteBuf> clazz) {
        // Unwrap if needed
        assertTrue(clazz.isInstance(buffer instanceof SimpleLeakAwareByteBuf ? buffer.unwrap() : buffer));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUsedDirectMemory() {
        T allocator =  newAllocator(true);
        ByteBufAllocatorMetric metric = ((ByteBufAllocatorMetricProvider) allocator).metric();
        assertEquals(0, metric.usedDirectMemory());
        ByteBuf buffer = allocator.directBuffer(1024, 4096);
        int capacity = buffer.capacity();
        assertEquals(expectedUsedMemory(allocator, capacity), metric.usedDirectMemory());

        // Double the size of the buffer
        buffer.capacity(capacity << 1);
        capacity = buffer.capacity();
        assertEquals(buffer.toString(), expectedUsedMemory(allocator, capacity), metric.usedDirectMemory());

        buffer.release();
        assertEquals(expectedUsedMemoryAfterRelease(allocator, capacity), metric.usedDirectMemory());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUsedHeapMemory() {
        T allocator =  newAllocator(true);
        ByteBufAllocatorMetric metric = ((ByteBufAllocatorMetricProvider) allocator).metric();

        assertEquals(0, metric.usedHeapMemory());
        ByteBuf buffer = allocator.heapBuffer(1024, 4096);
        int capacity = buffer.capacity();
        assertEquals(expectedUsedMemory(allocator, capacity), metric.usedHeapMemory());

        // Double the size of the buffer
        buffer.capacity(capacity << 1);
        capacity = buffer.capacity();
        assertEquals(expectedUsedMemory(allocator, capacity), metric.usedHeapMemory());

        buffer.release();
        assertEquals(expectedUsedMemoryAfterRelease(allocator, capacity), metric.usedHeapMemory());
    }

    protected long expectedUsedMemory(T allocator, int capacity) {
        return capacity;
    }

    protected long expectedUsedMemoryAfterRelease(T allocator, int capacity) {
        return 0;
    }

    public static void main(String[] args){
        int pageSize = 8 * 1024;
        int mask = ~(pageSize - 1);
        int z = 0xFFFFFE00;
        System.err.println(z);
        System.err.println(Integer.toBinaryString(z));
        System.err.println(mask);
        System.err.println(-pageSize == mask);
        System.err.println(Integer.toBinaryString(pageSize));
        System.err.println(Integer.toBinaryString(-pageSize));
        System.err.println(Integer.toBinaryString(mask));


        System.err.println(0x4000000000000000L);
        System.err.println(new BigDecimal(Math.pow(2,62)).toPlainString());
    }
}
