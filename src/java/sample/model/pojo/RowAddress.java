package sample.model.pojo;

import java.io.Serializable;

public class RowAddress implements Serializable {
    private static final long serialVersionUID = 8553571533963610104L;
    private final String filePath;
    private long position;
    private int size;
    private final RowAddress next;

    private RowAddress(String filePath, long position, int size, RowAddress next) {
        this.filePath = filePath;
        this.position = position;
        this.size = size;
        this.next = next;
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

    public RowAddress getNext() {
        return next;
    }
}
