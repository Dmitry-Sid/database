package server.model.impl;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.DataCompressor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DataCompressorImpl implements DataCompressor {
    private static final Logger log = LoggerFactory.getLogger(DataCompressorImpl.class);

    @Override
    public byte[] compress(byte[] input) {
        try (ByteArrayOutputStream bous = new ByteArrayOutputStream(input.length)) {
            try (OutputStream outputStream = compress(bous)) {
                outputStream.write(input);
            }
            return bous.toByteArray();
        } catch (Exception e) {
            log.error("error while compressing data", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public OutputStream compress(OutputStream output) {
        try {
            return new GZIPOutputStream(output);
        } catch (Exception e) {
            log.error("error while compressing inputStream", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] decompress(byte[] input) {
        try (InputStream inputStream = decompress(new ByteArrayInputStream(input))) {
            return IOUtils.toByteArray(inputStream);
        } catch (Exception e) {
            log.error("error while compressing data", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream decompress(InputStream input) {
        try {
            return new GZIPInputStream(input);
        } catch (Exception e) {
            log.error("error while decompressing inputStream", e);
            throw new RuntimeException(e);
        }
    }
}
