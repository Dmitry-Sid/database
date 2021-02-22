package sample.model;

import sample.model.pojo.RowAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileHelper {

    public void collectFile(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer);

    public interface InputOutputConsumer {
        void accept(InputStream inputStream, OutputStream outputStream) throws IOException;
    }
}
