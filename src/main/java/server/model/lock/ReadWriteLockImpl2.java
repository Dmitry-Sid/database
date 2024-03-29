package server.model.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class ReadWriteLockImpl2<T> extends BaseReadWriteLock<T> {
    private static final Logger log = LoggerFactory.getLogger(ReadWriteLockImpl2.class);

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

    private class InnerLock extends BaseInnerLock {
        private final Function<java.util.concurrent.locks.ReadWriteLock, java.util.concurrent.locks.Lock> function;

        private InnerLock(Type type) {
            super(type);
            this.function = Type.Read == type ? java.util.concurrent.locks.ReadWriteLock::readLock : java.util.concurrent.locks.ReadWriteLock::writeLock;
        }

        @Override
        protected void innerLock(T value) {
            try {
                if (value == null) {
                    throw new IllegalArgumentException("value cannot be null");
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
            } catch (Exception e) {
                log.error("error while locking value " + value, e);
                throw e;
            }
        }

        @Override
        protected void innerUnLock(T value) {
            try {
                if (value == null) {
                    throw new IllegalArgumentException("value cannot be null");
                }
                java.util.concurrent.locks.ReadWriteLock readWriteLock;
                synchronized (LOCK) {
                    readWriteLock = map.get(value);
                    if (readWriteLock == null) {
                        throw new IllegalStateException("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
                    }
                }
                function.apply(readWriteLock).unlock();
            } catch (Exception e) {
                log.error("error while unlocking value " + value, e);
                throw e;
            }
        }
    }
}
