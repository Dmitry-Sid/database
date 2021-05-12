package server.model;

import server.model.pojo.SimpleCondition;

import java.util.Set;

public abstract class FieldKeeper<U extends Comparable<U>, V> implements Destroyable {
    private final String fieldName;
    protected final String fileName;
    protected final ObjectConverter objectConverter;

    protected FieldKeeper(String fieldName, String fileName, ObjectConverter objectConverter) {
        this.fieldName = fieldName;
        this.fileName = fileName;
        this.objectConverter = objectConverter;
    }

    public void transform(U oldKey, U key, V value) {
        if (oldKey != null && oldKey.equals(key)) {
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
