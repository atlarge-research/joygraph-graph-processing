package io.joygraph.core.util;

import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DirectByteBufferGrowingOutputStream extends OutputStream {

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
        PlatformDependent.freeDirectBuffer(buf);
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

    private ByteBuffer allocateDirectBufferZeroed(int capacity) {
        return ByteBuffer.allocateDirect(capacity);
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
        PlatformDependent.freeDirectBuffer(buf);
    }

    public int size() {
        return buf.duplicate().flip().remaining();
    }
}
