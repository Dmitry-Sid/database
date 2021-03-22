package sample.model.pojo;

import java.io.Serializable;

public abstract class TableType implements Serializable {
    protected int id;

    public synchronized void setId(int id) {
        this.id = id;
    }

    public synchronized int getId() {
        return id;
    }

}
