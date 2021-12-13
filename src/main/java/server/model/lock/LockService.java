package server.model.lock;

import java.util.function.Supplier;

public class LockService {
    private static final ReadWriteLock<String> fileReadWriteLock = createReadWriteLock(String.class);

    public static ReadWriteLock<String> getFileReadWriteLock() {
        return fileReadWriteLock;
    }

    public static <T> Lock<T> createLock(Class<T> clazz) {
        return new LockImpl<>();
    }

    public static <T> ReadWriteLock<T> createReadWriteLock(Class<T> clazz) {
        return new ReadWriteLockImpl<>();
    }

    public static <U> void doInLock(Lock<U> lock, U value, Runnable runnable) {
        doInLock(lock, value, () -> {
            runnable.run();
            return null;
        });
    }

    public static <U, T> T doInLock(Lock<U> lock, U value, Supplier<T> supplier) {
        lock.lock(value);
        try {
            return supplier.get();
        } finally {
            lock.unlock(value);
        }
    }

    public static void doInReadWriteLock(java.util.concurrent.locks.Lock lock, Runnable runnable) {
        doInReadWriteLock(lock, () -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T doInReadWriteLock(java.util.concurrent.locks.Lock lock, Supplier<T> supplier) {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }
}