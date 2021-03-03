package sample.model;

import sample.model.pojo.ICondition;

import java.util.Set;

public interface IndexService {

    public SearchResult search(ICondition iCondition);

    public class SearchResult {
        public final boolean found;
        public final Set<Integer> idSet;

        public SearchResult(boolean found, Set<Integer> idSet) {
            this.found = found;
            this.idSet = idSet;
        }
    }
}
