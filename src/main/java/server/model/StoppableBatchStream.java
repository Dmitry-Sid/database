package server.model;

public interface StoppableBatchStream<T> extends StoppableStream<T> {

    void addOnBatchEnd(Runnable action);

}
