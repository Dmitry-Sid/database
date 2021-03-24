package server.model.pojo;

import java.util.Objects;

public class RowAddress extends TableType {
    private static final long serialVersionUID = 8553571533963610104L;
    private final String filePath;
    private final int id;
    private long position;
    private int size;
    private RowAddress previous;
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

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public RowAddress getPrevious() {
        return previous;
    }

    public void setPrevious(RowAddress previous) {
        this.previous = previous;
    }

    public RowAddress getNext() {
        return next;
    }

    public void setNext(RowAddress next) {
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
        return Objects.hash(getFilePath(), getId(), getPosition(), getSize());
    }
}
