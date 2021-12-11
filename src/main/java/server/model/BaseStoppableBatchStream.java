package server.model;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseStoppableBatchStream<T> extends BaseStoppableStream<T> implements StoppableBatchStream<T> {
    protected final List<Runnable> onBatchEnd = new ArrayList<>();

    @Override
    public void addOnBatchEnd(Runnable action) {
        onBatchEnd.add(action);
    }
}
