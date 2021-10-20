package server.model.impl;

import server.model.AsyncDestroyable;
import server.model.DestroyService;
import server.model.Destroyable;

import java.util.ArrayList;
import java.util.List;

public class DestroyServiceImpl extends AsyncDestroyable implements DestroyService {
    private final long sleepTime;
    private final List<Destroyable> destroyableList = new ArrayList<>();

    public DestroyServiceImpl(long sleepTime) {
        this.sleepTime = sleepTime;
        startDestroy(sleepTime);
    }

    @Override
    public synchronized void register(Destroyable destroyable) {
        destroyableList.add(destroyable);
    }

    @Override
    public synchronized void unregister(Destroyable destroyable) {
        destroyableList.remove(destroyable);
    }

    @Override
    public long getSleepTime() {
        return sleepTime;
    }

    @Override
    public synchronized void destroy() {
        destroyableList.forEach(Destroyable::destroy);
    }
}
