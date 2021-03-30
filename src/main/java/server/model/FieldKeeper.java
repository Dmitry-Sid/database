package server.model;

import server.model.pojo.SimpleCondition;

import java.io.Serializable;
import java.util.Set;

public abstract class FieldKeeper<U extends Comparable, V> implements Serializable {
    private final String fieldName;

    protected FieldKeeper(String fieldName) {
        this.fieldName = fieldName;
    }

    public void transform(U oldKey, U key, V value) {
        if (oldKey == null || key == null || oldKey.equals(key)) {
            return;
        }
        if (delete(oldKey, value)) {
            insert(key, value);
        }
    }

    public abstract void insert(U key, V value);

    public abstract boolean delete(U key, V value);

    public abstract Set<V> search(ConditionService conditionService, SimpleCondition condition);

    public abstract Set<V> search(U key);

    public String getFieldName() {
        return fieldName;
    }
}
