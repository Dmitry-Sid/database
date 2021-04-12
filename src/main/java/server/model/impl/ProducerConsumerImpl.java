package server.model.impl;

import server.model.ProducerConsumer;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class ProducerConsumerImpl<T> implements ProducerConsumer<T> {
    private final int maxSize;
    private final Queue<T> queue;

    public ProducerConsumerImpl(int maxSize) {
        this.maxSize = maxSize;
        this.queue = new ArrayBlockingQueue<>(maxSize);
    }


    @Override
    public synchronized void put(T value) {
        while (queue.size() >= maxSize) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        queue.add(value);
        notifyAll();
    }

    @Override
    public synchronized T take() {
        while (queue.size() <= 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        final T value = queue.poll();
        notifyAll();
        return value;
    }

    @Override
    public synchronized int size() {
        return queue.size();
    }
}
