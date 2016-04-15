package io.joygraph.core.util;

import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DirectByteBufferGrowingOutputStream extends OutputStream {

    protected ByteBuffer buf;
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    public DirectByteBufferGrowingOutputStream(int byteBufferSize) {
        buf = ByteBuffer.allocateDirect(byteBufferSize);
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
        // TODO could use unsafe to reallocate memory, but then need to adjust capacity of the ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(newCapacity);
        buf.flip();
        byteBuffer.put(buf);

        // free directbuffer
        // TODO remove hard dependency on netty
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

    synchronized public void trim() {
        // if it's not full, we can trim to save space
        if (buf.position() < buf.capacity()) {
            ByteBuffer newBuf = ByteBuffer.allocateDirect(buf.position());
            ByteBuffer dup = buf.duplicate();
            dup.flip();
            newBuf.put(dup);
            PlatformDependent.freeDirectBuffer(buf);
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
