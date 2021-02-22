package sample.model;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class Lock<T> {
    final Logger log = LoggerFactory.getLogger(getClass());

    private final Set<T> lockedObjects = new HashSet<>();
    private final ThreadLocal<T> threadLocal = new ThreadLocal<>();

    public synchronized void lock(T value) {
        if (value.equals(threadLocal.get())) {
            log.warn("try to acquire lock second time : " + threadLocal.get() + ", thread : " + Thread.currentThread());
            return;
        }
        while (true) {
            if (lockedObjects.contains(value)) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }
        }
        lockedObjects.add(value);
        threadLocal.set(value);
    }

    public synchronized void unlock(T value) {
        if (!value.equals(threadLocal.get())) {
            log.warn("try to unlock not acquired lock : " + threadLocal.get() + ", thread : " + Thread.currentThread());
            return;
        }
        lockedObjects.remove(value);
        threadLocal.set(null);
        notify();
    }

}
