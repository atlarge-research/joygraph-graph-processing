package io.joygraph.core.util;

import io.netty.util.internal.PlatformDependent;
import sun.misc.Unsafe;
import sun.misc.VM;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DirectByteBufferGrowingOutputStreamNonManaged extends OutputStream {
    private static Unsafe UNSAFE;
    private static Field capacityField;
    private static Field addressField;

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
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }

        try {
            addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            capacityField = Buffer.class.getDeclaredField("capacity");
            capacityField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    protected ByteBuffer buf;
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    public DirectByteBufferGrowingOutputStreamNonManaged(int byteBufferSize) {
        buf = allocateDirectBufferNonZeroed(byteBufferSize);
    }

    public boolean isEmpty() {
        return buf.position() == 0;
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
        long address = PlatformDependent.directBufferAddress(buf);
        long size = alignSizeToPage(newCapacity);
        long base = UNSAFE.reallocateMemory(address, size);
        long newAddress = addressFromBase(base);
        try {
            addressField.setLong(buf, newAddress);
            capacityField.setInt(buf, newCapacity);
        } catch (IllegalAccessException e) {
            // noop
        }

        buf.limit(newCapacity);
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

    private long addressFromBase(long base) {
        boolean pa = VM.isDirectMemoryPageAligned();
        int ps = UNSAFE.pageSize();
        long address;
        if (pa && (base % ps != 0)) {
            // Round up to page boundary
            address = base + ps - (base & (ps - 1));
        } else {
            address = base;
        }
        return address;
    }

    private long alignSizeToPage(int capacity) {
        boolean pa = VM.isDirectMemoryPageAligned();
        int ps = UNSAFE.pageSize();
        return Math.max(1L, (long)capacity + (pa ? ps : 0));
    }

    private ByteBuffer allocateDirectBufferNonZeroed(int capacity) {
        ByteBuffer bb = ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder());

        long size = alignSizeToPage(capacity);

        long base = 0;
        try {
            base = UNSAFE.allocateMemory(size);
        } catch (OutOfMemoryError x) {
            throw x;
        }
        long address = addressFromBase(base);

        try {
            addressField.setLong(bb, address);
            capacityField.setInt(bb, capacity);
        } catch (IllegalAccessException e) {
            // noop
        }
        bb.limit(capacity);
        return bb;
    }

    synchronized public void trim() {
        // if it's not full, we can trim to save space
        if (buf.position() < buf.capacity()) {
            ByteBuffer newBuf = allocateDirectBufferNonZeroed(buf.position());
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
        PlatformDependent.freeDirectBuffer(buf);
    }

    public int size() {
        return buf.duplicate().flip().remaining();
    }
}
