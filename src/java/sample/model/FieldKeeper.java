package sample.model;

import sample.model.pojo.SimpleCondition;

import java.util.Set;

public abstract class FieldKeeper<U extends Comparable, V> {
    private final String fieldName;
    final ConditionService conditionService;

    protected FieldKeeper(String fieldName, ConditionService conditionService) {
        this.fieldName = fieldName;
        this.conditionService = conditionService;
    }

    public void transform(U oldKey, U key, V value) {
        delete(oldKey, value);
        insert(key, value);
    }

    public abstract void insert(U key, V value);

    public abstract void delete(U key, V value);

    public abstract Set<V> search(SimpleCondition condition);

    public abstract Set<V> search(U key);

    public String getFieldName() {
        return fieldName;
    }
}
