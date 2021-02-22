package sample.model;

import java.io.*;

public class ObjectConverterImpl implements ObjectConverter {

    @Override
    public <T> T fromFile(Class<T> clazz, String file) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            return (T) fromObjectInputStream(ois);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T fromBytes(Class<T> clazz, byte[] bytes) {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (T) fromObjectInputStream(ois);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private Object fromObjectInputStream(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        return ois.readObject();
    }

    @Override
    public byte[] toBytes(Serializable serializable) {
        try (ByteArrayOutputStream baous = new ByteArrayOutputStream(); ObjectOutputStream ous = new ObjectOutputStream(baous)) {
            ous.writeObject(serializable);
            return baous.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void toFile(Serializable serializable, String file) {
        try (FileOutputStream fous = new FileOutputStream(file); ObjectOutputStream ous = new ObjectOutputStream(fous)) {
            ous.writeObject(serializable);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
