package server.model.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLockImpl2<T> implements ReadWriteLock<T> {
    private static final Logger log = LoggerFactory.getLogger(ReadWriteLockImpl2.class);
    private final Map<T, java.util.concurrent.locks.ReadWriteLock> map = new HashMap<>();
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

    private class InnerReadLock implements Lock<T> {
        @Override
        public void lock(T value) {
            java.util.concurrent.locks.ReadWriteLock readWriteLock;
            synchronized (ReadWriteLockImpl2.this) {
                readWriteLock = map.get(value);
                if (readWriteLock == null) {
                    readWriteLock = new ReentrantReadWriteLock();
                    map.put(value, readWriteLock);
                }
            }
            readWriteLock.readLock().lock();
        }

        @Override
        public void unlock(T value) {
            java.util.concurrent.locks.ReadWriteLock readWriteLock;
            synchronized (ReadWriteLockImpl2.this) {
                readWriteLock = map.get(value);
                if (readWriteLock == null) {
                    log.warn("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
                    return;
                }
            }
            readWriteLock.readLock().unlock();
        }
    }

    private class InnerWriteLock implements Lock<T> {
        @Override
        public void lock(T value) {
            java.util.concurrent.locks.ReadWriteLock readWriteLock;
            synchronized (ReadWriteLockImpl2.this) {
                readWriteLock = map.get(value);
                if (readWriteLock == null) {
                    readWriteLock = new ReentrantReadWriteLock();
                    map.put(value, readWriteLock);
                }
            }
            readWriteLock.writeLock().lock();
        }

        @Override
        public void unlock(T value) {
            java.util.concurrent.locks.ReadWriteLock readWriteLock;
            synchronized (ReadWriteLockImpl2.this) {
                readWriteLock = map.get(value);
                if (readWriteLock == null) {
                    log.warn("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
                    return;
                }
            }
            readWriteLock.writeLock().unlock();
        }
    }
}
