package sample.model;

import sample.model.pojo.ICondition;

public interface Tree<V> {

    public void transform(Comparable oldValue, Comparable value, int id);

    public void insert(Comparable value, int id);

    public void delete(Comparable value, int id);

    public Node<V> search(ICondition iCondition);

    public static class Node<V> {
        public final Comparable key;
        public final V value;

        public Node(Comparable key, V value) {
            this.key = key;
            this.value = value;
        }
    }
}
