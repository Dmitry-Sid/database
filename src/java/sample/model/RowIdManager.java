package sample.model;

import sample.model.pojo.RowAddress;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public interface RowIdManager {

    public boolean process(int id, Consumer<RowAddress> rowAddressConsumer);

    public void stream(Consumer<RowAddress> rowAddressConsumer);

    public void transform(int id, int newSize);

    public void add(Function<RowAddress, Boolean> function);

    public void delete(int id);

    public List<RowAddressGroup> groupAndSort(List<RowAddress> rowAddresses);

    public class RowAddressGroup {
        public final String fileName;
        public final List<RowAddress> rowAddresses;

        public RowAddressGroup(String fileName, List<RowAddress> rowAddresses) {
            this.fileName = fileName;
            this.rowAddresses = rowAddresses;
        }
    }

}
