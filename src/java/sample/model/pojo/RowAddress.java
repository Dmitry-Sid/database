package sample.model.pojo;

import java.io.Serializable;
import java.util.Objects;

public class RowAddress implements Serializable {
    private static final long serialVersionUID = 8553571533963610104L;
    private final String filePath;
    private final int id;
    private long position;
    private int size;
    private RowAddress next;

    public RowAddress(String filePath, int id, long position, int size) {
        this.filePath = filePath;
        this.id = id;
        this.position = position;
        this.size = size;
    }

    public String getFilePath() {
        return filePath;
    }

    public synchronized long getPosition() {
        return position;
    }

    public synchronized void setPosition(long position) {
        this.position = position;
    }

    public synchronized int getSize() {
        return size;
    }

    public synchronized void setSize(int size) {
        this.size = size;
    }

    public synchronized RowAddress getNext() {
        return next;
    }

    public synchronized void setNext(RowAddress next) {
        this.next = next;
    }

    public int getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowAddress)) {
            return false;
        }
        final RowAddress that = (RowAddress) o;
        return getId() == that.getId() &&
                getPosition() == that.getPosition() &&
                getSize() == that.getSize() &&
                Objects.equals(getFilePath(), that.getFilePath());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFilePath(), getId(), getPosition(), getSize(), getNext());
    }
}
