import org.junit.Test;
import server.model.ProducerConsumer;
import server.model.impl.ProducerConsumerImpl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ProducerConsumerTest {

    @Test
    public void singleThreadTest() {
        final ProducerConsumer<String> producerConsumer = new ProducerConsumerImpl<>(3);
        assertEquals(0, producerConsumer.size());
        producerConsumer.put("test1");
        assertEquals(1, producerConsumer.size());
        assertEquals("test1", producerConsumer.take());
        assertEquals(0, producerConsumer.size());
        producerConsumer.put("test2");
        producerConsumer.put("test3");
        producerConsumer.put("test4");
        assertEquals(3, producerConsumer.size());
        assertEquals("test2", producerConsumer.take());
        assertEquals(2, producerConsumer.size());
        assertEquals("test3", producerConsumer.take());
        assertEquals(1, producerConsumer.size());
        assertEquals("test4", producerConsumer.take());
        assertEquals(0, producerConsumer.size());
    }

    @Test
    public void emptyTest() {
        final ProducerConsumer<String> producerConsumer = new ProducerConsumerImpl<>(3);
        final long begin = System.currentTimeMillis();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producerConsumer.put("test1");
        }).start();
        assertEquals("test1", producerConsumer.take());
        assertTrue(System.currentTimeMillis() - begin >= 1000);
    }

    @Test
    public void fullTest() {
        final ProducerConsumer<String> producerConsumer = new ProducerConsumerImpl<>(3);
        final long begin = System.currentTimeMillis();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            assertEquals("test1", producerConsumer.take());
        }).start();
        producerConsumer.put("test1");
        producerConsumer.put("test2");
        producerConsumer.put("test3");
        producerConsumer.put("test4");
        assertTrue(System.currentTimeMillis() - begin >= 1000);
        assertEquals("test2", producerConsumer.take());
        assertEquals("test3", producerConsumer.take());
        assertEquals("test4", producerConsumer.take());
    }

}
