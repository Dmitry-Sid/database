import org.junit.Test;
import server.model.lock.Lock;
import server.model.lock.SingleLock;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SingleLockTest {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        {
            final Lock<Integer> lock = new SingleLock<>();
            final Set<Integer> set = Collections.synchronizedSet(new HashSet<>());
            final SingleLockBean<Integer> singleLockBean = new SingleLockBean<>(executorService, set);
            TestUtils.lock(lock, set, 1);
            final Future<Long> future1 = singleLockBean.run(lock, 2, null, true);
            final Future<Long> future2 = singleLockBean.run(lock, 1, null, false);
            Thread.sleep(1000);
            TestUtils.unlock(lock, set, 1);
            future1.get();
            future2.get();


            TestUtils.lock(lock, set, 3);
            final Future<Long> future3 = singleLockBean.run(lock, 3, "test exception", false);
            final Future<Long> future4 = singleLockBean.run(lock, 4, null, true);
            Thread.sleep(1000);
            TestUtils.unlock(lock, set, 3);
            try {
                future3.get();
                fail("no way");
            } catch (Exception e) {
                assertEquals("java.lang.RuntimeException: test exception", e.getMessage());
            }
            future4.get();
        }
        {
            final Lock<String> lock = new SingleLock<>();
            final Set<String> set = Collections.synchronizedSet(new HashSet<>());
            final SingleLockBean<String> singleLockBean = new SingleLockBean<>(executorService, set);
            TestUtils.lock(lock, set, "test1");
            final Future<Long> future1 = singleLockBean.run(lock, "test2", null, true);
            final Future<Long> future2 = singleLockBean.run(lock, "test1", null, false);
            Thread.sleep(1000);
            TestUtils.unlock(lock, set, "test1");
            future1.get();
            future2.get();
        }
    }

    private static class SingleLockBean<T> extends TestUtils.LockBean<T> {

        private SingleLockBean(ExecutorService executorService, Set<T> lockedValues) {
            super(executorService, lockedValues);
        }

        public Future<Long> run(Lock<T> lock, T value, String exception, boolean gottaWait) {
            return executorService.submit(() -> {
                final long begin = System.currentTimeMillis();
                synchronized (SingleLockBean.this) {
                    if (gottaWait && lockedValues.contains(value)) {
                        fail("lock failed");
                    }
                }
                lock.lock(value);
                synchronized (SingleLockBean.this) {
                    lockedValues.add(value);
                }
                try {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if (exception != null) {
                        throw new RuntimeException(exception);
                    }
                    return System.currentTimeMillis() - begin;
                } finally {
                    synchronized (SingleLockBean.this) {
                        lockedValues.remove(value);
                    }
                    lock.unlock(value);
                }
            });
        }
    }
}
