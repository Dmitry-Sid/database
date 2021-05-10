import org.junit.Test;
import server.model.Buffer;
import server.model.impl.BufferImpl;
import server.model.pojo.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class BufferTest {

    @Test
    public void fullTest() {
        fullTest(10);
        fullTest(100);
        fullTest(1000);
    }

    private void fullTest(int maxSize) {
        final AtomicInteger flushedConsumerCounter = new AtomicInteger();
        final Buffer<Row> buffer = new BufferImpl<>(maxSize, list -> {
            list.forEach(value -> flushedConsumerCounter.incrementAndGet());
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
            assertFalse(value.isFlushed());
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize - 1, counter.get());
        assertEquals(maxSize - 1, buffer.size());
        assertEquals(0, flushedConsumerCounter.get());

        Row row = new Row(0, new HashMap<>());
        buffer.add(row, Buffer.State.DELETED);
        buffer.flush();
        counter.set(0);
        previous.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertTrue(value.isFlushed());
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize - 2, counter.get());
        assertEquals(maxSize - 2, buffer.size());
        {
            assertNull(buffer.get(0));
        }
        assertEquals(maxSize - 1, flushedConsumerCounter.get());

        row = new Row(maxSize - 1, new HashMap<>());
        buffer.add(row, Buffer.State.ADDED);
        counter.set(0);
        previous.set(0);
        final AtomicInteger flushed = new AtomicInteger();
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            if (value.isFlushed()) {
                flushed.incrementAndGet();
            }
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize - 1, counter.get());
        assertEquals(maxSize - 1, buffer.size());
        assertEquals(maxSize - 2, flushed.get());
        {
            final Buffer.Element<Row> element = buffer.get(maxSize - 1);
            assertEquals(row, element.getValue());
            assertEquals(Buffer.State.ADDED, element.getState());
        }
        assertEquals(maxSize - 1, flushedConsumerCounter.get());

        row = new Row(maxSize, new HashMap<>());
        buffer.add(row, Buffer.State.UPDATED);
        counter.set(0);
        previous.set(0);
        flushed.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            if (value.isFlushed()) {
                flushed.incrementAndGet();
            }
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize, counter.get());
        assertEquals(maxSize, buffer.size());
        assertEquals(maxSize - 2, flushed.get());
        {
            final Buffer.Element<Row> element = buffer.get(maxSize);
            assertEquals(row, element.getValue());
            assertEquals(Buffer.State.UPDATED, element.getState());
        }
        assertEquals(maxSize - 1, flushedConsumerCounter.get());

        final Map<String, Comparable> map = new HashMap<>();
        map.put("print", Integer.toString(maxSize + 1));
        row = new Row(maxSize + 1, map);
        buffer.add(row, Buffer.State.UPDATED);
        counter.set(0);
        previous.set(0);
        flushed.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            if (value.isFlushed()) {
                flushed.incrementAndGet();
            }
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize + 1, counter.get());
        assertEquals(maxSize + 1, buffer.size());
        assertEquals(maxSize - 2, flushed.get());
        {
            final Buffer.Element<Row> element = buffer.get(maxSize + 1);
            assertEquals(row, element.getValue());
            assertEquals(Buffer.State.UPDATED, element.getState());
        }
        assertEquals(maxSize - 1, flushedConsumerCounter.get());

        buffer.flush();
        counter.set(0);
        previous.set(0);
        buffer.stream(value -> {
            counter.incrementAndGet();
            assertEquals(value, buffer.get(value.getValue().getId()));
            assertTrue(value.isFlushed());
            assertTrue(previous.get() <= value.getValue().getId());
            previous.set(value.getValue().getId());
        });
        assertEquals(maxSize, counter.get());
        assertEquals(maxSize, buffer.size());
        assertEquals(maxSize + 2, flushedConsumerCounter.get());
    }
}
