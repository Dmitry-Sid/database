package sample.model;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Lock<T> {
    private static final Logger log = LoggerFactory.getLogger(Lock.class);

    private final Set<T> lockedObjects = new HashSet<>();
    private final ThreadLocal<Map<T, Integer>> threadLocal = ThreadLocal.withInitial(HashMap::new);

    public synchronized void lock(T value) {
        while (true) {
            final Integer count = threadLocal.get().get(value);
            if (lockedObjects.contains(value) && (count == null || count <= 0)) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }
        }
        final Integer count = threadLocal.get().get(value);
        threadLocal.get().put(value, count == null ? 1 : count + 1);
        if (threadLocal.get().get(value) == 1) {
            lockedObjects.add(value);
        }
    }

    public synchronized void unlock(T value) {
        final Integer count = threadLocal.get().get(value);
        if (count != null && count <= 0) {
            log.warn("try to unlock not acquired lock : " + threadLocal.get() + ", count : " + count + ", thread : " + Thread.currentThread());
            return;
        }
        if (count == null || count == 1) {
            threadLocal.get().remove(value);
            lockedObjects.remove(value);
            notify();
        } else {
            threadLocal.get().put(value, count - 1);
        }
    }
}
