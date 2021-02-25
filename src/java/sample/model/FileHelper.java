package sample.model;

import sample.model.pojo.RowAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileHelper {

    public void write(String fileName, byte[] bytes, boolean append);

    public byte[] read(RowAddress rowAddress);

    public void collect(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer);

    public interface InputOutputConsumer {
        void accept(InputStream inputStream, OutputStream outputStream) throws IOException;
    }
}
