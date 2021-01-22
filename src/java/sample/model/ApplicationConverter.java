package sample.model;

import sample.model.pojo.Application;

public interface ApplicationConverter {
    public Application fromBytes(byte[] bytes);

    public byte[] toBytes(Application application);
}
