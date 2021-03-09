package sample.model;

import java.util.function.Supplier;

public class LockService {
    private static final Lock<Integer> rowIdLock = new Lock<>();
    private static final Lock<String> fileLock = new Lock<>();

    public static Lock<Integer> getRowIdLock() {
        return rowIdLock;
    }

    public static Lock<String> getFileLock() {
        return fileLock;
    }

    public static <T> T doInRowIdLock(int id, Supplier<T> supplier) {
        return doInLock(rowIdLock, id, supplier);
    }

    public static <T> T doInFileLock(String fileName, Supplier<T> supplier) {
        return doInLock(fileLock, fileName, supplier);
    }

    public static <T> Lock<T> createLock(Class<T> clazz) {
        return new Lock<>();
    }

    public static <U, T> T doInLock(Lock<U> lock, U value, Supplier<T> supplier) {
        lock.lock(value);
        try {
            return supplier.get();
        } finally {
            lock.unlock(value);
        }
    }

}
