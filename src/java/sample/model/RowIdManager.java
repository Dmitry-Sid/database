package sample.model;

import sample.model.pojo.RowAddress;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public interface RowIdManager {

    public boolean process(int id, Consumer<RowAddress> rowAddressConsumer);

    public void process(Set<Integer> inputSet, Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker);

    public void stream(Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker);

    public void transform(int id, int newSize);

    public void add(Function<RowAddress, Boolean> function);

    public void delete(int id);

}
