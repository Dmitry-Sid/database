package server.model;

import server.model.pojo.RowAddress;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;

public interface FileHelper {

    void write(String fileName, byte[] bytes, boolean append);

    byte[] read(RowAddress rowAddress);

    void skip(InputStream inputStream, long size);

    ChainStream<InputStream> getChainInputStream();

    ChainStream<OutputStream> getChainOutputStream();

    void collect(StoppableBatchStream<RowAddress> stream, Consumer<CollectBean> consumer);

    interface InputOutputConsumer {
        void accept(InputStream inputStream, OutputStream outputStream) throws IOException;
    }

    interface ChainStream<T extends Closeable> extends Chained<String> {
        T getStream();
    }

    class CollectBean {
        public final RowAddress rowAddress;
        public final InputStream inputStream;
        public final OutputStream outputStream;

        public CollectBean(RowAddress rowAddress, InputStream inputStream, OutputStream outputStream) {
            this.rowAddress = rowAddress;
            this.inputStream = inputStream;
            this.outputStream = outputStream;
        }
    }
}
