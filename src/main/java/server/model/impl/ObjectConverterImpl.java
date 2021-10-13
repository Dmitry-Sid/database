package server.model.impl;

import org.apache.commons.lang3.SerializationUtils;
import server.model.DataCompressor;
import server.model.ObjectConverter;

import java.io.*;

public class ObjectConverterImpl implements ObjectConverter {
    private final DataCompressor dataCompressor;

    public ObjectConverterImpl(DataCompressor dataCompressor) {
        this.dataCompressor = dataCompressor;
    }

    @Override
    public <T extends Serializable> T fromFile(Class<T> clazz, String file) {
        if (!new File(file).exists()) {
            return null;
        }
        try (InputStream inputStream = dataCompressor.decompress(new FileInputStream(file))) {
            return SerializationUtils.deserialize(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T extends Serializable> T fromBytes(Class<T> clazz, byte[] bytes) {
        return SerializationUtils.deserialize(dataCompressor.decompress(bytes));
    }

    @Override
    public byte[] toBytes(Serializable serializable) {
        return dataCompressor.compress(SerializationUtils.serialize(serializable));
    }

    @Override
    public void toFile(Serializable serializable, String file) {
        try (final OutputStream outputStream = dataCompressor.compress(new FileOutputStream(file))) {
            outputStream.write(SerializationUtils.serialize(serializable));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T extends Serializable> T clone(T object) {
        return SerializationUtils.clone(object);
    }
}
