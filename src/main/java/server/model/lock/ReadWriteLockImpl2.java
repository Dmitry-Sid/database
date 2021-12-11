package server.model.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class ReadWriteLockImpl2<T> implements ReadWriteLock<T> {
    private static final Logger log = LoggerFactory.getLogger(ReadWriteLockImpl2.class);
    private static final boolean DEBUG_MODE = false;

    private final Object LOCK = new Object();
    private final Map<T, java.util.concurrent.locks.ReadWriteLock> map = new HashMap<>();
    private final Lock<T> readLock = new InnerLock(Type.Read);
    private final Lock<T> writeLock = new InnerLock(Type.Write);

    @Override
    public Lock<T> readLock() {
        return readLock;
    }

    @Override
    public Lock<T> writeLock() {
        return writeLock;
    }

    private String printStackTrace() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Thread ").append(Thread.currentThread().getName());
        for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
            sb.append("\t").append(ste).append("\n");
        }
        return sb.toString();
    }

    private enum Type {
        Read, Write
    }

    private class InnerLock implements Lock<T> {
        private final Type type;
        private final Function<java.util.concurrent.locks.ReadWriteLock, java.util.concurrent.locks.Lock> function;

        private InnerLock(Type type) {
            this.type = type;
            this.function = Type.Read == type ? java.util.concurrent.locks.ReadWriteLock::readLock : java.util.concurrent.locks.ReadWriteLock::writeLock;
        }

        @Override
        public void lock(T value) {
            if (DEBUG_MODE) {
                System.out.println("try to acquire lock, value " + value + ", type " + type + ", " + printStackTrace());
            }
            java.util.concurrent.locks.ReadWriteLock readWriteLock;
            synchronized (LOCK) {
                readWriteLock = map.get(value);
                if (readWriteLock == null) {
                    readWriteLock = new ReentrantReadWriteLock();
                    map.put(value, readWriteLock);
                }
            }
            function.apply(readWriteLock).lock();
            if (DEBUG_MODE) {
                System.out.println("lock acquired, value " + value + ", type " + type + ", " + printStackTrace());
            }
        }

        @Override
        public void unlock(T value) {
            if (DEBUG_MODE) {
                System.out.println("try to release lock, value " + value + ", type " + type + ", " + printStackTrace());
            }
            try {
                java.util.concurrent.locks.ReadWriteLock readWriteLock;
                synchronized (LOCK) {
                    readWriteLock = map.get(value);
                    if (readWriteLock == null) {
                        throw new IllegalStateException("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
                    }
                }
                function.apply(readWriteLock).unlock();
            } catch (Exception e) {
                log.error("error", e);
            }
            if (DEBUG_MODE) {
                System.out.println("lock released, value " + value + ", type " + type + ", " + printStackTrace());
            }
        }
    }
}
