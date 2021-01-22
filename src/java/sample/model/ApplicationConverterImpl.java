package sample.model;

import sample.model.pojo.Application;

import java.io.*;

public class ApplicationConverterImpl implements ApplicationConverter {
    @Override
    public Application fromBytes(byte[] bytes) {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (Application) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] toBytes(Application application) {
        try (ByteArrayOutputStream baous = new ByteArrayOutputStream(); ObjectOutputStream ous = new ObjectOutputStream(baous)) {
            ous.writeObject(application);
            return baous.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
