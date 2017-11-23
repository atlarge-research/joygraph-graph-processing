package io.joygraph.core.util;

import io.joygraph.core.util.collection.OHCWrapper;
import io.netty.util.internal.PlatformDependent;
import sun.misc.Unsafe;
import sun.misc.VM;

import javax.management.RuntimeErrorException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class DirectByteBufferGrowingOutputStream extends OutputStream {

    private static Unsafe UNSAFE;
    private static Method UNRESERVE;
    private static Method RESERVE;

    static {
        try {
            Unsafe unsafeInstance = null;
            for (Field field : Unsafe.class.getDeclaredFields()) {
                if (field.getType() == Unsafe.class) {
                    field.setAccessible(true);
                    unsafeInstance = (Unsafe) field.get(null);
                    break;
                }
            }
            if (unsafeInstance == null) throw new IllegalStateException("");
            else UNSAFE = unsafeInstance;

            try {
                Class<?> bitsClass = Class.forName("java.nio.Bits");
                UNRESERVE = bitsClass.getDeclaredMethod("unreserveMemory", Long.TYPE, Integer.TYPE);
                UNRESERVE.setAccessible(true);

                RESERVE = bitsClass.getDeclaredMethod("reserveMemory", Long.TYPE, Integer.TYPE);
                RESERVE.setAccessible(true);

            } catch (ClassNotFoundException | NoSuchMethodException e) {
                e.printStackTrace();
            }
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }


    protected ByteBuffer buf;
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    public DirectByteBufferGrowingOutputStream() {
        // you need to use setBuf to use it
    }

    public DirectByteBufferGrowingOutputStream(int byteBufferSize) {
        buf = allocateDirectBufferZeroed(byteBufferSize);
    }

    public boolean isEmpty() {
        return buf.position() == 0;
    }

    /**
     * Don't use unless you know what you're doing
     * This is directly getting the buf instead of returning a duplicate
     * Basically this doesn't create a new instance.
     */
    public ByteBuffer getBufDirect() {
        return this.buf;
    }

    public void setBuf(ByteBuffer buf) {
        this.buf = buf;
    }

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity - buf.capacity() > 0) {
            grow(minCapacity);
        }
    }

    private void grow(int minCapacity) {
        int oldCapacity = buf.capacity();
        int newCapacity = oldCapacity << 1;
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        // allocate new
        ByteBuffer byteBuffer = allocateDirectBufferZeroed(newCapacity);
        long srcAddress = PlatformDependent.directBufferAddress(buf);
        long dstAddress = PlatformDependent.directBufferAddress(byteBuffer);
        PlatformDependent.copyMemory(srcAddress, dstAddress, buf.position());
        byteBuffer.position(buf.position());
        destroy();
        buf = byteBuffer;
    }

    public synchronized void write(ByteBuffer byteBuffer) {
        // reserve bytes
        ensureCapacity(buf.position() + byteBuffer.remaining());
        buf.put(byteBuffer);
    }

    public ByteBuffer getBuf() {
        return buf.duplicate();
    }

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException("Unsupported write(int), use write(ByteBuffer)");
    }

    private static void reserveMemory(long size, int cap) {
        try {
            RESERVE.invoke(null, size, cap);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("reserve failed");
        }
    }

    private static void unreserveMemory(long size, int cap) {
        try {
            UNRESERVE.invoke(null, size, cap);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("unreserve failed");
        }
    }

    private ByteBuffer allocateDirectBufferZeroed(int capacity) {
        // No cleaner, we just do it ourselves
        // but still keep track of memory usage
        boolean pa = VM.isDirectMemoryPageAligned();
        int ps = UNSAFE.pageSize();

        long size = Math.max(1L, (long)capacity + (pa ? ps : 0));
        reserveMemory(size, capacity);

        long base = 0;
        try {
            base = PlatformDependent.allocateMemory(size);
        } catch (OutOfMemoryError x) {
            unreserveMemory(size, capacity);
            throw x;
        }


        UNSAFE.setMemory(base, size, (byte) 0);

        long address;

        if (pa && (base % ps != 0)) {
            // Round up to page boundary
            address = base + ps - (base & (ps - 1));
        } else {
            address = base;
        }

        long[] baseAndSize = {base, size};

        return OHCWrapper.instantiate(baseAndSize, address, capacity);
    }

    synchronized public void trim() {
        // if it's not full, we can trim to save space
        if (buf.position() < buf.capacity()) {
            ByteBuffer newBuf = allocateDirectBufferZeroed(buf.position());
            ByteBuffer dup = buf.duplicate();
            dup.flip();
            newBuf.put(dup);
            destroy();
            buf = newBuf;
        }
    }

    synchronized public void clear() {
        buf.clear();
    }

    synchronized public void destroy() {
        // check if it has a cleaner... if it does we didn't allocate it. PROBABLY


        // since we put base in att, we abuse it to get base addr
        long[] baseAndSize;
        try {
            baseAndSize = (long[]) ByteBufferUtil.ATTFIELD.get(buf);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Should never happen", e);
        }
        if (baseAndSize == null) {
            PlatformDependent.freeDirectBuffer(buf);
        } else {
            PlatformDependent.freeMemory(baseAndSize[0]);
            try {
                UNRESERVE.invoke(null, baseAndSize[1], buf.capacity());
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Should never happen", e);
            }
        }
    }

    public int size() {
        return buf.duplicate().flip().remaining();
    }
}
