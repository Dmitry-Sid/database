package server.model;

public interface ProducerConsumer<T> {

    void put(T value);

    T take();

    int size();

}
