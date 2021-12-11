package server.model;

import server.model.pojo.RowAddress;

import java.util.Set;
import java.util.function.Consumer;

public interface RowIdRepository extends Repository<RowAddress> {

    int newId();

    void add(int id, Consumer<RowAddress> rowAddressConsumer);

    StoppableBatchStream<RowAddress> batchStream();

    StoppableBatchStream<RowAddress> batchStream(Type type, Set<Integer> idSet);

    String getRowFileName(int rowId);

    enum Type {
        Read, Write
    }
}