package sample.model.lock;

import java.util.function.Supplier;

public class LockService {
    public static <T> Lock<T> createLock(Class<T> clazz) {
        return new Lock<>();
    }

    public static <T> ReadWriteLock<T> createReadWriteLock(Class<T> clazz) {
        return new ReadWriteLock<>();
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

    public static <U> void doInReadWriteLock(ReadWriteLock<U> lock, LockType lockType, U value, Runnable runnable) {
        lock(lock, lockType, value);
        try {
            runnable.run();
        } finally {
            unlock(lock, lockType, value);
        }
    }

    public static <U, T> T doInReadWriteLock(ReadWriteLock<U> lock, LockType lockType, U value, Supplier<T> supplier) {
        lock(lock, lockType, value);
        try {
            return supplier.get();
        } finally {
            unlock(lock, lockType, value);
        }
    }

    private static <U> void lock(ReadWriteLock<U> lock, LockType lockType, U value) {
        switch (lockType) {
            case Read:
                lock.readLock(value);
                break;
            case Write:
                lock.writeLock(value);
                break;
        }
    }

    private static <U> void unlock(ReadWriteLock<U> lock, LockType lockType, U value) {
        switch (lockType) {
            case Read:
                lock.readUnlock(value);
                break;
            case Write:
                lock.writeUnlock(value);
                break;
        }
    }

    public enum LockType {
        Read, Write
    }

}
