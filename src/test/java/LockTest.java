import org.junit.Test;
import server.model.lock.Lock;
import server.model.lock.LockImpl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LockTest {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        {
            final Lock<Integer> lock = new LockImpl<>();
            final Set<Integer> set = Collections.synchronizedSet(new HashSet<>());
            final TestUtils.LockBean<Integer> lockBean = new TestUtils.LockBean<>(executorService, set);
            TestUtils.lock(lock, set, 1);
            final Future<Long> future1 = lockBean.run(lock, 1, null, true);
            final Future<Long> future2 = lockBean.run(lock, 2, null, false);
            final Future<Long> future3 = lockBean.run(lock, 1, null, true);
            Thread.sleep(1000);
            TestUtils.unlock(lock, set, 1);
            future1.get();
            future2.get();
            future3.get();

            TestUtils.lock(lock, set, 3);
            final Future<Long> future4 = lockBean.run(lock, 3, "test exception", true);
            final Future<Long> future5 = lockBean.run(lock, 3, null, true);
            Thread.sleep(1000);
            TestUtils.unlock(lock, set, 3);
            try {
                future4.get();
                fail("no way");
            } catch (Exception e) {
                assertEquals("java.lang.RuntimeException: test exception", e.getMessage());
            }
            future5.get();
        }
        {
            final Lock<String> lock = new LockImpl<>();
            final Set<String> set = Collections.synchronizedSet(new HashSet<>());
            final TestUtils.LockBean<String> lockBean = new TestUtils.LockBean<>(executorService, set);
            TestUtils.lock(lock, set, "test1");
            final Future<Long> future1 = lockBean.run(lock, "test1", null, true);
            final Future<Long> future2 = lockBean.run(lock, "test2", null, false);
            final Future<Long> future3 = lockBean.run(lock, "test1", null, true);
            Thread.sleep(1000);
            TestUtils.unlock(lock, set, "test1");
            future1.get();
            future2.get();
            future3.get();
        }
    }
}
