package server.model.pojo;

import java.util.Objects;

public class RowAddress extends TableType {
    private static final long serialVersionUID = 8553571533963610104L;
    private final String filePath;
    private long position;
    private int size;
    private int previous = -1;
    private int next = -1;
    private boolean saved;

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

    public int getPrevious() {
        return previous;
    }

    public void setPrevious(int previous) {
        this.previous = previous;
    }

    public int getNext() {
        return next;
    }

    public void setNext(int next) {
        this.next = next;
    }

    public boolean isSaved() {
        return saved;
    }

    public void setSaved(boolean saved) {
        this.saved = saved;
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
