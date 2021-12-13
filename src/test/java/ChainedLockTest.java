import org.junit.Test;
import server.model.ChainedLock;
import server.model.lock.Lock;
import server.model.lock.LockImpl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class ChainedLockTest {
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        final Lock<Integer> lock = new LockImpl<>();
        final Set<Integer> set = Collections.synchronizedSet(new HashSet<>());
        final TestUtils.LockBean<Integer> lockBean = new TestUtils.LockBean<>(executorService, set);
        final ChainedLock<Integer> chainedLockRef;
        try (ChainedLock<Integer> chainedLock = new TestChainedLock<>(lock, set)) {
            chainedLockRef = chainedLock;
            {
                chainedLock.init(1);
                final Future<Long> future1 = lockBean.run(lock, 1, null, true);
                final Future<Long> future2 = lockBean.run(lock, 2, null, false);
                Thread.sleep(1000);
                chainedLock.init(2);
                future1.get();
                future2.get();
            }
            {
                final Future<Long> future1 = lockBean.run(lock, 1, null, false);
                final Future<Long> future2 = lockBean.run(lock, 2, null, true);
                Thread.sleep(1000);
                chainedLock.init(3);
                future1.get();
                future2.get();
            }
            {
                final Future<Long> future1 = lockBean.run(lock, 1, null, false);
                final Future<Long> future2 = lockBean.run(lock, 2, null, false);
                future1.get();
                future2.get();
            }
            {
                chainedLock.close();
                final Future<Long> future1 = lockBean.run(lock, 3, null, false);
                future1.get();
                chainedLock.init(4);
            }
            {
                final Future<Long> future1 = lockBean.run(lock, 4, null, true);
                final Future<Long> future2 = lockBean.run(lock, 5, null, false);
                Thread.sleep(1000);
                chainedLock.init(5);
                future1.get();
                future2.get();
            }
        }
        assertTrue(chainedLockRef.isClosed());
        final Future<Long> future1 = lockBean.run(lock, 5, null, false);
        Thread.sleep(1000);
        future1.get();
    }

    private static class TestChainedLock<T> extends ChainedLock<T> {
        private final Set<T> set;

        private TestChainedLock(Lock<T> lock, Set<T> set) {
            super(lock);
            this.set = set;
        }

        @Override
        public void init(T value) {
            super.init(value);
            set.add(value);
        }

        @Override
        public void close() {
            final T value = getValue();
            super.close();
            if (isClosed()) {
                set.remove(value);
            }
        }
    }
}
