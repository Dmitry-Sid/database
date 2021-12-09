package server.model;

import server.model.pojo.RowAddress;

import java.util.Set;
import java.util.function.Consumer;

public interface RowIdRepository extends Repository<RowAddress> {

    int newId();

    void add(int id, Consumer<RowAddress> rowAddressConsumer);

    StoppableStream<RowAddress> stream(Action action);

    StoppableStream<RowAddress> stream(Action action, Set<Integer> idSet);

    String getRowFileName(int rowId);

    enum Action {
        READ, WRITE
    }

}