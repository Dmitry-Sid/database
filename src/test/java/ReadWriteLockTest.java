import server.model.lock.ReadWriteLock;
import server.model.lock.ReadWriteLockImpl;

public class ReadWriteLockTest extends BaseReadWriteLockTest {
    @Override
    protected <T> ReadWriteLock<T> getReadWriteLock(Class<T> clazz) {
        return new ReadWriteLockImpl<>();
    }
}
