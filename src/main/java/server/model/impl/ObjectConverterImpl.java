package server.model.impl;

import org.apache.commons.lang3.SerializationUtils;
import server.model.ObjectConverter;

import java.io.*;

public class ObjectConverterImpl implements ObjectConverter {

    @Override
    public <T extends Serializable> T fromFile(Class<T> clazz, String file) {
        if (!new File(file).exists()) {
            return null;
        }
        try (InputStream inputStream = new FileInputStream(file)) {
            return SerializationUtils.deserialize(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T extends Serializable> T fromBytes(Class<T> clazz, byte[] bytes) {
        return SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(Serializable serializable) {
        return SerializationUtils.serialize(serializable);
    }

    @Override
    public void toFile(Serializable serializable, String file) {
        try (FileOutputStream fous = new FileOutputStream(file)) {
            fous.write(SerializationUtils.serialize(serializable));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T extends Serializable> T clone(T object) {
        return SerializationUtils.clone(object);
    }
}
