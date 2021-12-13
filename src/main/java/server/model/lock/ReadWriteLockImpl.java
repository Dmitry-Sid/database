package server.model.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ReadWriteLockImpl<T> extends BaseReadWriteLock<T> {
    private static final Logger log = LoggerFactory.getLogger(ReadWriteLockImpl.class);

    private final Object LOCK = new Object();
    private final Map<T, Counter> lockedObjects = new HashMap<>();
    private final ThreadLocal<Map<T, Counter>> threadLocal = ThreadLocal.withInitial(HashMap::new);
    private final Lock<T> readLock = new InnerLock(Type.Read, (local, global) -> global.getWriteCount().get() > local.getWriteCount().get(), Counter::getReadCount);
    private final Lock<T> writeLock = new InnerLock(Type.Write, (local, global) -> (local.getWriteCount().get() == 0 && local.getReadCount().get() > 0)
            || global.getWriteCount().get() > local.getWriteCount().get()
            || global.getReadCount().get() > local.getReadCount().get(), Counter::getWriteCount);

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

    private class InnerLock extends BaseInnerLock {
        private final WaitFunction waitFunction;
        private final Function<Counter, AtomicInteger> changeFunction;

        private InnerLock(Type type, WaitFunction waitFunction, Function<Counter, AtomicInteger> changeFunction) {
            super(type);
            this.waitFunction = waitFunction;

            this.changeFunction = changeFunction;
        }

        @Override
        protected void innerLock(T value) {
            synchronized (LOCK) {
                if (value == null) {
                    return;
                }
                Counter global;
                Counter local;
                while (true) {
                    global = lockedObjects.getOrDefault(value, new Counter());
                    local = threadLocal.get().getOrDefault(value, new Counter());
                    if (waitFunction.apply(local, global)) {
                        try {
                            LOCK.wait(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        break;
                    }
                }
                applyAndPutCounter(value, local, changeFunction, threadLocal.get());
                applyAndPutCounter(value, global, changeFunction, lockedObjects);
            }
        }

        private boolean isNullCounter(Counter counter) {
            return counter == null || (counter.readCount.get() <= 0 && counter.writeCount.get() <= 0);
        }

        private void applyAndPutCounter(T value, Counter counter, Function<Counter, AtomicInteger> changeFunction, Map<T, Counter> map) {
            changeFunction.apply(counter).incrementAndGet();
            map.put(value, counter);
        }

        @Override
        protected void innerUnLock(T value) {
            synchronized (LOCK) {
                if (value == null) {
                    return;
                }
                try {
                    final Counter local = threadLocal.get().get(value);
                    if (local == null || changeFunction.apply(local).get() <= 0) {
                        throw new IllegalStateException("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
                    } else {
                        applyAndRemoveCounter(value, local, threadLocal.get());
                        applyAndRemoveCounter(value, lockedObjects.get(value), lockedObjects);
                    }
                    LOCK.notifyAll();
                } catch (Exception e) {
                    log.error("error", e);
                    throw e;
                }
            }
        }

        private void applyAndRemoveCounter(T value, Counter counter, Map<T, Counter> map) {
            if (counter != null) {
                changeFunction.apply(counter).decrementAndGet();
            }
            if (isNullCounter(counter)) {
                map.remove(value);
            }
        }

        @Override
        protected synchronized String printAdditionalInfo(T value) {
            final Counter local = threadLocal.get().getOrDefault(value, new Counter());
            final Counter global = lockedObjects.getOrDefault(value, new Counter());
            return "local read: " + local.getReadCount() + ", local write: " + local.getWriteCount() + ", global read: " + global.getReadCount() + ", global write: " + global.getWriteCount() + ", ";
        }
    }

    interface WaitFunction {
        boolean apply(Counter local, Counter global);
    }
}
