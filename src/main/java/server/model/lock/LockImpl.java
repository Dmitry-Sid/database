package server.model.lock;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockImpl<T> implements Lock<T> {
    private static final Logger log = LoggerFactory.getLogger(LockImpl.class);

    private final Set<T> lockedObjects = new HashSet<>();
    private final ThreadLocal<Map<T, Integer>> threadLocal = ThreadLocal.withInitial(HashMap::new);

    @Override
    public synchronized void lock(T value) {
        try {
            if (value == null) {
                throw new IllegalArgumentException("value cannot be null");
            }
            Integer count;
            while (true) {
                count = threadLocal.get().get(value);
                if (lockedObjects.contains(value) && count == null) {
                    try {
                        wait(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    break;
                }
            }
            count = threadLocal.get().get(value);
            if (count == null) {
                count = 0;
            }
            threadLocal.get().put(value, ++count);
            if (threadLocal.get().get(value) == 1) {
                lockedObjects.add(value);
            }
        } catch (Exception e) {
            log.error("error while locking value " + value, e);
            throw e;
        }
    }

    @Override
    public synchronized void unlock(T value) {
        try {
            if (value == null) {
                throw new IllegalArgumentException("value cannot be null");
            }
            final Integer count = threadLocal.get().get(value);
            if (count == null || count <= 0) {
                throw new IllegalStateException("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
            }
            if (count == 1) {
                threadLocal.get().remove(value);
                lockedObjects.remove(value);
                notifyAll();
            } else {
                threadLocal.get().put(value, count - 1);
            }
        } catch (Exception e) {
            log.error("error while unlocking value " + value, e);
            throw e;
        }
    }
}
