package server.model;

import server.model.pojo.FieldCondition;

import java.util.Set;

public interface FieldKeeper<U extends Comparable<U>, V> extends Destroyable {

    void transform(U oldKey, U key, V value);

    void insert(U key, V value);

    DeleteResult delete(U key, V value);

    Set<V> conditionSearch(FieldCondition condition, int size);

    Set<V> search(U key);

    void clear();

    String getFieldName();

    class DeleteResult {
        public final boolean deleted;
        public final boolean fully;

        public DeleteResult(boolean deleted, boolean fully) {
            this.deleted = deleted;
            this.fully = fully;
        }
    }


}
