package sample.model;

import sample.model.pojo.SimpleCondition;

import java.util.Set;

public interface FieldKeeper<U extends Comparable, V> {

    public String getField();

    public void transform(U oldKey, U key, V value);

    public void insert(U key, V value);

    public void delete(U key, V value);

    public Set<V> search(SimpleCondition condition);

    public Set<V> search(U comparable);
}
