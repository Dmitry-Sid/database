package server.model;

import server.model.pojo.ICondition;
import server.model.pojo.Row;

import java.util.Set;

public interface IndexService {

    public SearchResult search(ICondition condition);

    public void transform(Row oldRow, Row row);

    public void insert(Row row);

    public void delete(Row row);

    public class SearchResult {
        public final boolean found;
        public final Set<Integer> idSet;

        public SearchResult(boolean found, Set<Integer> idSet) {
            this.found = found;
            this.idSet = idSet;
        }
    }
}
