package server.model;

public abstract class AsyncDestroyable implements Destroyable {

    protected void startDestroy(long sleepTime) {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                destroy();
            }
        }).start();
    }
}
