package io.joygraph.core.util;

import java.util.Iterator;

public class Iterators {

    public static <T> Iterator<T> wrap(Iterator<T>[] iterators) {
        return new IteratorsWrapper<>(iterators);
    }

    private static class IteratorsWrapper<T> implements Iterator<T> {
        private final Iterator<T>[] iterators;
        private int currentIndex = 0;

        public IteratorsWrapper(Iterator<T>[] iterators) {
            this.iterators = iterators;
        }

        private int whichHasNext(int index) {
            if (index >= iterators.length) {
                return -1;
            }

            if (iterators[index].hasNext()) {
                return index;
            } else {
                return whichHasNext(index + 1);
            }
        }

        @Override
        public boolean hasNext() {
            if (iterators[currentIndex].hasNext()) {
                return true;
            } else {
                int newIndex = whichHasNext(currentIndex);
                if (newIndex == -1) {
                    return false;
                } else {
                    currentIndex = newIndex;
                    return true;
                }
            }
        }

        @Override
        public T next() {
            return iterators[currentIndex].next();
        }
    }
}