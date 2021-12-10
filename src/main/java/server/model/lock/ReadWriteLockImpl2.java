package server.model.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class ReadWriteLockImpl2<T> implements ReadWriteLock<T> {
    private static final Logger log = LoggerFactory.getLogger(ReadWriteLockImpl2.class);
    private final Object LOCK = new Object();

    private final Map<T, java.util.concurrent.locks.ReadWriteLock> map = new HashMap<>();
    private final Lock<T> readLock = new InnerLock(java.util.concurrent.locks.ReadWriteLock::readLock);
    private final Lock<T> writeLock = new InnerLock(java.util.concurrent.locks.ReadWriteLock::writeLock);

    @Override
    public Lock<T> readLock() {
        return readLock;
    }

    @Override
    public Lock<T> writeLock() {
        return writeLock;
    }

    private class InnerLock implements Lock<T> {
        private final Function<java.util.concurrent.locks.ReadWriteLock, java.util.concurrent.locks.Lock> function;

        private InnerLock(Function<java.util.concurrent.locks.ReadWriteLock, java.util.concurrent.locks.Lock> function) {
            this.function = function;
        }

        @Override
        public void lock(T value) {
            java.util.concurrent.locks.ReadWriteLock readWriteLock;
            synchronized (LOCK) {
                readWriteLock = map.get(value);
                if (readWriteLock == null) {
                    readWriteLock = new ReentrantReadWriteLock();
                    map.put(value, readWriteLock);
                }
            }
            function.apply(readWriteLock).lock();
        }

        @Override
        public void unlock(T value) {
            java.util.concurrent.locks.ReadWriteLock readWriteLock;
            synchronized (LOCK) {
                readWriteLock = map.get(value);
                if (readWriteLock == null) {
                    log.warn("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
                    return;
                }
            }
            function.apply(readWriteLock).unlock();
        }
    }
}
