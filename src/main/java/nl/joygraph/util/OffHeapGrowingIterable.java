package nl.joygraph.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;

public class OffHeapGrowingIterable<V> implements Iterable<V> {

    private DirectByteBufferGrowingOutputStream outputStream;
    private int size = 0;

    private DirectByteBufferGrowingOutputStream getOutputStream() {
        if (outputStream == null) {
            outputStream = new DirectByteBufferGrowingOutputStream(32);
        }
        return outputStream;
    }

    public void add(ByteBuffer byteBuffer) throws IOException {
        add(byteBuffer, 1);
    }

    public void add(ByteBuffer byteBuffer, int n) throws IOException {
        getOutputStream().write(byteBuffer);
        size+=n;
    }

    @Override
    public Iterator<V> iterator() {
        return new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public V next() {
                return null;
            }
        };
    }

    @Override
    public Spliterator<V> spliterator() {
        return Spliterators.spliterator(iterator(), size, 0);
    }
}
