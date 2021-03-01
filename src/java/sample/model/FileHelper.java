package sample.model;

import sample.model.pojo.RowAddress;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileHelper {

    public void write(String fileName, byte[] bytes, boolean append);

    public byte[] read(RowAddress rowAddress);

    public ChainInputStream getChainInputStream();

    public void collect(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer);

    public interface InputOutputConsumer {
        void accept(InputStream inputStream, OutputStream outputStream) throws IOException;
    }

    public interface ChainInputStream extends Closeable {
        public void read(String fileName);

        public String getFileName();

        public InputStream getInputStream();

        public boolean isClosed();
    }
}
