package sample.model.pojo;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class Row implements Serializable {
    private static final long serialVersionUID = -172808855119652235L;

    private int id;
    private final Map<String, Object> fields;

    public Row(Map<String, Object> fields) {
        this.fields = fields;
    }

    public Row(int id, Map<String, Object> fields) {
        this.id = id;
        this.fields = fields;
    }

    public synchronized void setId(int id) {
        this.id = id;
    }

    public synchronized int getId() {
        return id;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Row)) {
            return false;
        }
        final Row row = (Row) o;
        return id == row.id &&
                Objects.equals(getFields(), row.getFields());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, getFields());
    }
}
