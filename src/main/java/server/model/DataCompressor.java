package server.model;

import java.io.InputStream;
import java.io.OutputStream;

public interface DataCompressor {
    byte[] compress(byte[] input);

    InputStream decompress(InputStream inputStream);

    byte[] decompress(byte[] input);

    OutputStream compress(OutputStream outputStream);
}
