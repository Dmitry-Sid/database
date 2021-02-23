package sample.model;

import sample.model.pojo.RowAddress;

import java.util.function.Consumer;

public interface RowIdManager {

    public int newId();

    public boolean process(int id, Consumer<RowAddress> rowAddressConsumer);

    public void transform(int id, int newSize);

    public void add(RowAddress rowAddress);

    public void delete(int id);

}
