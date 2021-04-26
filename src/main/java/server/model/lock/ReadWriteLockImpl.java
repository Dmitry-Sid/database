package server.model.lock;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ReadWriteLockImpl<T> implements ReadWriteLock<T> {
    public static final Object DEFAULT = new Object();
    private static final Logger log = LoggerFactory.getLogger(ReadWriteLockImpl.class);

    private final Map<T, Counter> lockedObjects = new HashMap<>();
    private final ThreadLocal<Map<T, Counter>> threadLocal = ThreadLocal.withInitial(HashMap::new);
    private final Lock<T> readLock = new InnerReadLock();
    private final Lock<T> writeLock = new InnerWriteLock();

    @Override
    public Lock<T> readLock() {
        return readLock;
    }

    @Override
    public Lock<T> writeLock() {
        return writeLock;
    }

    private static class Counter {
        private final AtomicInteger readCount = new AtomicInteger(0);
        private final AtomicInteger writeCount = new AtomicInteger(0);

        private AtomicInteger getReadCount() {
            return readCount;
        }

        private AtomicInteger getWriteCount() {
            return writeCount;
        }
    }

    private class InnerReadLock extends BaseInnerLock {

        @Override
        public synchronized void lock(T value) {
            lock(value, counter -> counter.getWriteCount().get() > 0, Counter::getReadCount);
        }

        @Override
        public synchronized void unlock(T value) {
            unlock(value, Counter::getReadCount);
        }
    }

    private class InnerWriteLock extends BaseInnerLock {

        @Override
        public synchronized void lock(T value) {
            lock(value, counter -> counter.getWriteCount().get() > 0 || counter.getReadCount().get() > 0, Counter::getWriteCount);
        }

        @Override
        public synchronized void unlock(T value) {
            unlock(value, Counter::getWriteCount);
        }
    }

    private abstract class BaseInnerLock implements Lock<T> {
        synchronized void lock(T value, Function<Counter, Boolean> waitFunction, Function<Counter, AtomicInteger> changeFunction) {
            if (value == null) {
                return;
            }
            Counter commonCounter = lockedObjects.get(value);
            Counter counter = threadLocal.get().get(value);
            while (true) {
                if (commonCounter != null && waitFunction.apply(commonCounter) && isNullCounter(counter)) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    break;
                }
            }
            if (counter == null) {
                counter = new Counter();
            }
            changeFunction.apply(counter).incrementAndGet();
            threadLocal.get().put(value, counter);
            if (commonCounter == null) {
                commonCounter = new Counter();
            }
            changeFunction.apply(commonCounter).incrementAndGet();
            lockedObjects.put(value, commonCounter);
        }

        synchronized void unlock(T value, Function<Counter, AtomicInteger> function) {
            if (value == null) {
                return;
            }
            final Counter counter = threadLocal.get().get(value);
            if (counter == null || function.apply(counter).get() <= 0) {
                log.warn("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
                threadLocal.get().remove(value);
            }
            if (counter != null) {
                function.apply(counter).decrementAndGet();
            }
            if (isNullCounter(counter)) {
                threadLocal.get().remove(value);
            }
            final Counter commonCounter = lockedObjects.get(value);
            if (commonCounter != null) {
                function.apply(commonCounter).decrementAndGet();
            }
            if (isNullCounter(commonCounter)) {
                lockedObjects.remove(value);
            }
            notifyAll();
        }

        private boolean isNullCounter(Counter counter) {
            return counter == null || (counter.readCount.get() <= 0 && counter.writeCount.get() <= 0);
        }
    }
}
