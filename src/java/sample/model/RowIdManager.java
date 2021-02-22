package sample.model;

import sample.model.pojo.RowAddress;

import java.util.function.Consumer;

public interface RowIdManager {

    public int newId();

    public boolean process(int id, Consumer<RowAddress> rowAddressConsumer);

    public void transform(RowAddress rowAddress, int sizeBefore, int sizeAfter);

    public boolean add(int id, Consumer<RowAddress> rowAddressConsumer);

}
