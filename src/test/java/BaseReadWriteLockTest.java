import org.junit.Test;
import server.model.lock.ReadWriteLock;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class BaseReadWriteLockTest {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        {
            final ReadWriteLock<Integer> lock = getReadWriteLock(Integer.class);
            final Set<Integer> set = Collections.synchronizedSet(new HashSet<>());
            final TestUtils.LockBean<Integer> lockBean = new TestUtils.LockBean<>(executorService, set);
            TestUtils.lock(lock.readLock(), set, 1);
            final Future<Long> future1 = lockBean.run(lock.readLock(), 1, null, false);
            final Future<Long> future2 = lockBean.run(lock.readLock(), 2, null, false);
            final Future<Long> future3 = lockBean.run(lock.readLock(), 1, null, false);
            Thread.sleep(1000);
            TestUtils.unlock(lock.readLock(), set, 1);
            future1.get();
            future2.get();
            future3.get();
        }
        {
            final ReadWriteLock<Integer> lock = getReadWriteLock(Integer.class);
            final Set<Integer> set = Collections.synchronizedSet(new HashSet<>());
            final TestUtils.LockBean<Integer> lockBean = new TestUtils.LockBean<>(executorService, set);
            TestUtils.lock(lock.readLock(), set, 1);
            final Future<Long> future1 = lockBean.run(lock.writeLock(), 1, null, true);
            final Future<Long> future2 = lockBean.run(lock.writeLock(), 2, null, false);
            final Future<Long> future3 = lockBean.run(lock.writeLock(), 1, null, true);
            Thread.sleep(1000);
            TestUtils.unlock(lock.readLock(), set, 1);
            future1.get();
            future2.get();
            future3.get();
        }
        {
            final ReadWriteLock<Integer> lock = getReadWriteLock(Integer.class);
            final Set<Integer> set = Collections.synchronizedSet(new HashSet<>());
            final TestUtils.LockBean<Integer> lockBean = new TestUtils.LockBean<>(executorService, set);
            TestUtils.lock(lock.writeLock(), set, 1);
            final Future<Long> future1 = lockBean.run(lock.readLock(), 1, null, true);
            final Future<Long> future2 = lockBean.run(lock.readLock(), 2, null, false);
            Thread.sleep(1000);
            TestUtils.unlock(lock.writeLock(), set, 1);
            future1.get();
            future2.get();
        }
        {
            final ReadWriteLock<Integer> lock = getReadWriteLock(Integer.class);
            final Set<Integer> set = Collections.synchronizedSet(new HashSet<>());
            final TestUtils.LockBean<Integer> lockBean = new TestUtils.LockBean<>(executorService, set);
            TestUtils.lock(lock.writeLock(), set, 1);
            final Future<Long> future1 = lockBean.run(lock.writeLock(), 1, null, true);
            final Future<Long> future2 = lockBean.run(lock.writeLock(), 2, null, false);
            final Future<Long> future3 = lockBean.run(lock.writeLock(), 1, null, true);
            Thread.sleep(1000);
            TestUtils.unlock(lock.writeLock(), set, 1);
            future1.get();
            future2.get();
            future3.get();
        }
    }

    protected abstract <T> ReadWriteLock<T> getReadWriteLock(Class<T> clazz);
}
