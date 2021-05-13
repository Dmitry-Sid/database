package server.model;

import server.model.pojo.SimpleCondition;

import java.util.Set;

public interface FieldKeeper<U extends Comparable<U>, V> extends Destroyable {

    void transform(U oldKey, U key, V value);

    void insert(U key, V value);

    boolean delete(U key, V value);

    Set<V> search(ConditionService conditionService, SimpleCondition condition);

    Set<V> search(U key);

    void clear();

    String getFieldName();
}
