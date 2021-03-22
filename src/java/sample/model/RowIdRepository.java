package sample.model;

import sample.model.pojo.RowAddress;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public interface RowIdRepository extends Repository<RowAddress> {

    public int newId();

    public void add(int id, Consumer<RowAddress> rowAddressConsumer);

    public void stream(Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker, Set<Integer> idSet);

}
