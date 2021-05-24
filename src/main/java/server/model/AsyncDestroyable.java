package server.model;

import server.model.impl.ProducerConsumerImpl;

public abstract class AsyncDestroyable implements Destroyable {
    protected final ProducerConsumer<Runnable> producerConsumer = new ProducerConsumerImpl<>(1000);
    protected volatile boolean destroyed;
    private volatile Runnable destroyMethod;

    protected void startDestroy(Runnable destroyMethod, long sleepTime) {
        this.destroyMethod = destroyMethod;
        new Thread(() -> {
            while (!destroyed && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (destroyed || Thread.currentThread().isInterrupted()) {
                    break;
                }
                producerConsumer.put(destroyMethod);
            }
        }).start();
        new Thread(() -> {
            while (true) {
                producerConsumer.take().run();
            }
        }).start();
    }

    @Override
    public void destroy() {
        destroyMethod.run();
        destroyed = true;
    }
}
