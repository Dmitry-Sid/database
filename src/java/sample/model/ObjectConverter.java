package sample.model;

import java.io.Serializable;

public interface ObjectConverter {
    public <T> T fromFile(Class<T> clazz, String file);

    public <T> T fromBytes(Class<T> clazz, byte[] bytes);

    public byte[] toBytes(Serializable serializable);

    public void toFile(Serializable serializable, String file);

    public <T extends Serializable> T clone(T object);
}
