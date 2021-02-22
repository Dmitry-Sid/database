package sample.model;

import java.util.HashSet;
import java.util.Set;

public class Lock<T> {
    private final Set<T> lockedObjects = new HashSet<>();

    public synchronized void lock(T value) {
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
    }

    public synchronized void unlock(T value) {
        lockedObjects.remove(value);
        notify();
    }

}
