package server.model;

public abstract class AsyncDestroyable implements Destroyable {
    private static final long MICRO_SLEEP = 100;

    protected volatile boolean isAwaken;

    protected void startDestroy(long sleepTime) {
        new Thread(() -> {
            while (true) {
                long sleptTime = 0;
                while (true) {
                    if (isAwaken) {
                        isAwaken = false;
                        break;
                    }
                    try {
                        Thread.sleep(MICRO_SLEEP);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    sleptTime += MICRO_SLEEP;
                    if (sleptTime >= sleepTime) {
                        break;
                    }
                }
                destroy();
            }
        }).start();
    }


    public void wakeUp() {
        isAwaken = true;
    }
}
