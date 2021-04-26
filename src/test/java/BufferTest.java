import org.junit.Test;
import server.model.Buffer;
import server.model.impl.BufferImpl;
import server.model.pojo.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BufferTest {

    @Test
    public void fullTest() {
        fullTest(10);
        fullTest(100);
        fullTest(1000);
    }

    private void fullTest(int maxSize) {
        final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        readWriteLock.readLock().lock();
        final Buffer<Row> buffer = new BufferImpl<>(maxSize, Runnable::run, list -> {
            list.forEach(value -> {
                if (value.getValue().getId() % maxSize == 0) {
                    System.out.println(value.getValue().getId());
                }
                if (value.getValue().getFields().containsKey("print")) {
                    System.out.println("passed");
                }
            });
        });
        for (int i = 0; i < maxSize - 1; i++) {
            final Map<String, Comparable> map = new HashMap<>();
            map.put("field", Integer.toString(i));
            buffer.add(new Row(i, map), Buffer.State.UPDATED);
        }
        assertEquals(maxSize - 1, buffer.size());
        for (int i = 0; i < maxSize - 1; i++) {
            final Map<String, Comparable> map = new HashMap<>();
            map.put("field", Integer.toString(i));
            assertEquals(new Buffer.Element<>(new Row(i, map), Buffer.State.UPDATED), buffer.get(i));
        }
        final AtomicInteger counter = new AtomicInteger();
        final AtomicInteger previous = new AtomicInteger();
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize - 1, counter.get());
        assertEquals(maxSize - 1, buffer.size());

        Row row = new Row(0, new HashMap<>());
        buffer.add(row, Buffer.State.DELETED);
        buffer.flush();
        counter.set(0);
        previous.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize - 1, counter.get());
        assertEquals(maxSize - 1, buffer.size());
        assertEquals(new Buffer.Element<>(row, Buffer.State.DELETED), buffer.get(0));

        row = new Row(maxSize - 1, new HashMap<>());
        buffer.add(row, Buffer.State.DELETED);
        buffer.flush();
        counter.set(0);
        previous.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize, counter.get());
        assertEquals(maxSize, buffer.size());
        assertEquals(new Buffer.Element<>(row, Buffer.State.DELETED), buffer.get(maxSize - 1));

        row = new Row(maxSize, new HashMap<>());
        buffer.add(row, Buffer.State.UPDATED);
        buffer.flush();
        counter.set(0);
        previous.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize, counter.get());
        assertEquals(maxSize, buffer.size());
        assertEquals(new Buffer.Element<>(row, Buffer.State.UPDATED), buffer.get(maxSize));

        final Map<String, Comparable> map = new HashMap<>();
        map.put("print", Integer.toString(maxSize + 1));
        row = new Row(maxSize + 1, map);
        buffer.add(row, Buffer.State.UPDATED);
        buffer.flush();
        counter.set(0);
        previous.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize, counter.get());
        assertEquals(maxSize, buffer.size());
        assertEquals(new Buffer.Element<>(row, Buffer.State.UPDATED), buffer.get(maxSize + 1));

        buffer.flush();
        counter.set(0);
        previous.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize, counter.get());
        assertEquals(maxSize, buffer.size());
        readWriteLock.readLock().unlock();
    }
}
