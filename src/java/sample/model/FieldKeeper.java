package sample.model;

import sample.model.pojo.SimpleCondition;

import java.util.Set;

public interface FieldKeeper<V> {

    public String getField();

    public void transform(Comparable oldKey, Comparable key, V value);

    public void insert(Comparable key, V value);

    public void delete(Comparable key, V value);

    public Set<V> search(SimpleCondition condition);
}
