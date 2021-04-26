package server.model.lock;

import java.util.function.Supplier;

public class LockService {
    public static <T> Lock<T> createLock(Class<T> clazz) {
        return new LockImpl<>();
    }

    public static <T> ReadWriteLock<T> createReadWriteLock(Class<T> clazz) {
        return new ReadWriteLockImpl2<>();
    }

    public static <U, T> T doInLock(Lock<U> lock, U value, Supplier<T> supplier) {
        lock.lock(value);
        try {
            return supplier.get();
        } finally {
            lock.unlock(value);
        }
    }

    public static <U> void doInLock(Lock<U> lock, U value, Runnable runnable) {
        lock.lock(value);
        try {
            runnable.run();
        } finally {
            lock.unlock(value);
        }
    }

    public static void doInReadWriteLock(java.util.concurrent.locks.ReadWriteLock lock, LockType lockType, Runnable runnable) {
        lock(lock, lockType);
        try {
            runnable.run();
        } finally {
            unlock(lock, lockType);
        }
    }

    public static <T> T doInReadWriteLock(java.util.concurrent.locks.ReadWriteLock lock, LockType lockType, Supplier<T> supplier) {
        lock(lock, lockType);
        try {
            return supplier.get();
        } finally {
            unlock(lock, lockType);
        }
    }

    private static void lock(java.util.concurrent.locks.ReadWriteLock lock, LockType lockType) {
        switch (lockType) {
            case Read:
                lock.readLock().lock();
                break;
            case Write:
                lock.writeLock().lock();
                break;
        }
    }

    private static void unlock(java.util.concurrent.locks.ReadWriteLock lock, LockType lockType) {
        switch (lockType) {
            case Read:
                lock.readLock().unlock();
                break;
            case Write:
                lock.writeLock().unlock();
                break;
        }
    }

    public enum LockType {
        Read, Write
    }
}
