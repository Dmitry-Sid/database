package sample.model.pojo;

import java.util.Map;
import java.util.Objects;

public class Row extends TableType {
    private static final long serialVersionUID = -172808855119652235L;

    private final Map<String, Comparable> fields;

    public Row(Map<String, Comparable> fields) {
        this.fields = fields;
    }

    public Row(int id, Map<String, Comparable> fields) {
        this.id = id;
        this.fields = fields;
    }

    public Map<String, Comparable> getFields() {
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
