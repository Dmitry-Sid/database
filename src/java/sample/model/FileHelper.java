package sample.model;

import sample.model.pojo.RowAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public interface FileHelper {

    public void write(String fileName, byte[] bytes, boolean append);

    public byte[] read(RowAddress rowAddress);

    public void read(List<RowAddress> rowAddresses, Consumer<byte[]> consumer, AtomicBoolean stopChecker);

    public void collect(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer);

    public interface InputOutputConsumer {
        void accept(InputStream inputStream, OutputStream outputStream) throws IOException;
    }
}
