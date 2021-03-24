package server.model.lock;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ReadWriteLock<T> {
    public static final Object DEFAULT = new Object();
    private static final Logger log = LoggerFactory.getLogger(Lock.class);

    private final Map<T, Counter> lockedObjects = new HashMap<>();
    private final ThreadLocal<Map<T, Counter>> threadLocal = ThreadLocal.withInitial(HashMap::new);

    public synchronized void readLock(T value) {
        lock(value, counter -> counter.getWriteCount().get() > 0, Counter::getReadCount);
    }

    public synchronized void readUnlock(T value) {
        unlock(value, Counter::getReadCount);
    }

    public synchronized void writeLock(T value) {
        lock(value, counter -> counter.getWriteCount().get() > 0 || counter.getReadCount().get() > 0, Counter::getWriteCount);
    }

    public synchronized void writeUnlock(T value) {
        unlock(value, Counter::getWriteCount);
    }

    private synchronized void lock(T value, Function<Counter, Boolean> waitFunction, Function<Counter, AtomicInteger> changeFunction) {
        if (value == null) {
            return;
        }
        Counter commonCounter;
        Counter counter;
        while (true) {
            commonCounter = lockedObjects.get(value);
            counter = threadLocal.get().get(value);
            if (commonCounter != null && waitFunction.apply(commonCounter) && counter == null) {
                try {
                    wait(100);
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

    private synchronized void unlock(T value, Function<Counter, AtomicInteger> function) {
        if (value == null) {
            return;
        }
        final Counter counter = threadLocal.get().get(value);
        if (counter == null || function.apply(counter).get() <= 0) {
            log.warn("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
            return;
        }
        function.apply(counter).decrementAndGet();
        final Counter commonCounter = lockedObjects.get(value);
        if (commonCounter != null) {
            function.apply(commonCounter).decrementAndGet();
        }
        if (commonCounter == null || (commonCounter.readCount.get() == 0 && commonCounter.writeCount.get() == 0)) {
            threadLocal.get().remove(value);
            lockedObjects.remove(value);
            notifyAll();
        } else {
            threadLocal.get().put(value, counter);
        }
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
}
