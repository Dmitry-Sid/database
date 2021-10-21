package server.model;

import server.model.pojo.ICondition;
import server.model.pojo.Row;

import java.util.Set;
import java.util.function.Consumer;

public interface IndexService extends Destroyable {

    SearchResult search(ICondition condition, int size);

    void transform(Row oldRow, Row row);

    void insert(Row row);

    void insert(Row row, Set<String> fields);

    void delete(Row row);

    void subscribeOnNewIndexes(Consumer<Set<String>> fieldsConsumer);

    class SearchResult {
        public final boolean found;
        public final Set<Integer> idSet;

        public SearchResult(boolean found, Set<Integer> idSet) {
            this.found = found;
            this.idSet = idSet;
        }
    }
}
