package server.model;

public interface DestroyService extends Destroyable {
    void register(Destroyable destroyable);

    void unregister(Destroyable destroyable);

    long getSleepTime();

    void wakeUp();
}
