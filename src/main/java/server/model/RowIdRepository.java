package server.model;

import server.model.pojo.RowAddress;

import java.util.Set;
import java.util.function.Consumer;

public interface RowIdRepository extends Repository<RowAddress> {

    int newId();

    void add(int id, Consumer<RowAddress> rowAddressConsumer);

    StoppableStream<RowAddress> stream();

    StoppableStream<RowAddress> stream(Set<Integer> idSet);

    StoppableStream<RowAddress> batchStream(Set<Integer> idSet, Runnable afterAction);

    String getRowFileName(int rowId);

}