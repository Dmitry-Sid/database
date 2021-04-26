package server.model.lock;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockImpl<T> implements Lock<T> {
    private static final Logger log = LoggerFactory.getLogger(LockImpl.class);

    private final Set<T> lockedObjects = new HashSet<>();
    private final ThreadLocal<Map<T, Integer>> threadLocal = ThreadLocal.withInitial(HashMap::new);

    public synchronized void lock(T value) {
        if (value == null) {
            return;
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
    }

    public synchronized void unlock(T value) {
        if (value == null) {
            return;
        }
        final Integer count = threadLocal.get().get(value);
        if (count == null || count <= 0) {
            log.warn("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
            return;
        }
        if (count == 1) {
            threadLocal.get().remove(value);
            lockedObjects.remove(value);
            notifyAll();
        } else {
            threadLocal.get().put(value, count - 1);
        }
    }
}
