package server.model;

import java.io.Serializable;

public interface ObjectConverter {
    <T extends Serializable> T fromFile(Class<T> clazz, String file);

    <T extends Serializable> T fromBytes(Class<T> clazz, byte[] bytes);

    byte[] toBytes(Serializable serializable);

    void toFile(Serializable serializable, String file);

    <T extends Serializable> T clone(T object);
}
