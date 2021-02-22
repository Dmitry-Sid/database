package sample.model;

public class LockKeeper {
    private static final Lock<Integer> rowIdLock = new Lock<>();
    private static final Lock<String> fileLock = new Lock<>();

    public static Lock<Integer> getRowIdLock() {
        return rowIdLock;
    }

    public static Lock<String> getFileLock() {
        return fileLock;
    }
}
