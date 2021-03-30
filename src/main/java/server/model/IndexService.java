package server.model;

import server.model.pojo.ICondition;
import server.model.pojo.Row;

import java.util.Set;

public interface IndexService {

    SearchResult search(ICondition condition);

    void transform(Row oldRow, Row row);

    void insert(Row row);

    void delete(Row row);

    void subscribeOnIndexesChanges(Runnable runnable);

    class SearchResult {
        public final boolean found;
        public final Set<Integer> idSet;

        public SearchResult(boolean found, Set<Integer> idSet) {
            this.found = found;
            this.idSet = idSet;
        }
    }
}
