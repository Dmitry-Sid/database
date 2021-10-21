import server.model.lock.ReadWriteLock;
import server.model.lock.ReadWriteLockImpl2;

public class ReadWriteLockTest2 extends BaseReadWriteLockTest {
    @Override
    protected <T> ReadWriteLock<T> getReadWriteLock(Class<T> clazz) {
        return new ReadWriteLockImpl2<>();
    }
}