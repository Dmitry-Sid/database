package sample.model;

import sample.model.pojo.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class IndexServiceImpl implements IndexService {
    private static final SearchResult EMPTY = new SearchResult(false, Collections.emptySet());
    private final Map<String, FieldKeeper> fieldKeepers;

    public IndexServiceImpl(Map<String, FieldKeeper> fieldKeepers) {
        this.fieldKeepers = fieldKeepers;
    }

    @Override
    public SearchResult search(ICondition condition) {
        if (condition == null || condition instanceof EmptyCondition) {
            return EMPTY;
        }
        return new SearchResult(true, searchIdSet(condition));
    }

    private Set<Integer> searchIdSet(ICondition condition) {
        if (condition instanceof SimpleCondition) {
            return fieldKeepers.get(((SimpleCondition) condition).getField()).search((SimpleCondition) condition);
        } else if (condition instanceof ComplexCondition) {
            return searchIdSet((ComplexCondition) condition);
        } else if (condition instanceof EmptyCondition) {
            return Collections.emptySet();
        }
        throw new ConditionException("Unknown condition class : " + condition.getClass());
    }

    private Set<Integer> searchIdSet(ComplexCondition condition) {
        Set<Integer> result = null;
        for (ICondition innerCondition : condition.getConditions()) {
            if (result == null) {
                result = searchIdSet(innerCondition);
                continue;
            }
            switch (condition.getType()) {
                case OR:
                    result.addAll(searchIdSet(innerCondition));
                    break;
                case AND:
                    result.retainAll(searchIdSet(innerCondition));
                    break;
                default:
                    throw new ConditionException("Unknown complex type : " + condition.getType());
            }
        }
        return result;
    }

    @Override
    public void transform(Row oldRow, Row row) {
        fieldKeepers.forEach((key, value) -> value.transform(oldRow.getFields().get(value.getFieldName()),
                row.getFields().get(value.getFieldName()), row.getId()));
    }

    @Override
    public void insert(Row row) {
        fieldKeepers.forEach((key, value) -> value.insert(row.getFields().get(value.getFieldName()), row.getId()));
    }

    @Override
    public void delete(Row row) {
        fieldKeepers.forEach((key, value) -> value.delete(row.getFields().get(value.getFieldName()), row.getId()));
    }
}
