package server.model;

import server.model.pojo.RowAddress;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public interface RowIdRepository extends Repository<RowAddress> {

    int newId();

    void add(int id, Consumer<RowAddress> rowAddressConsumer);

    void stream(Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker, Set<Integer> idSet);

    String getRowFileName(int rowId);

}
