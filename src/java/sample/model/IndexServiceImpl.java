package sample.model;

import sample.model.pojo.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexServiceImpl implements IndexService {
    private static final SearchResult EMPTY = new SearchResult(false, Collections.emptySet());
    private final ConditionService conditionService;
    private final Map<String, FieldKeeper<Integer>> fieldKeepers;

    public IndexServiceImpl(ConditionService conditionService, Map<String, FieldKeeper<Integer>> fieldKeepers) {
        this.conditionService = conditionService;
        this.fieldKeepers = fieldKeepers;
    }

    @Override
    public SearchResult search(ICondition condition) {
        if (condition instanceof EmptyCondition) {
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
        final Set<Integer> result = new HashSet<>();
        for (ICondition innerCondition : condition.getConditions()) {
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
        fieldKeepers.forEach((key, value) -> value.transform(oldRow.getFields().get(value.getField()),
                row.getFields().get(value.getField()), row.getId()));
    }

    @Override
    public void insert(Row row) {
        fieldKeepers.forEach((key, value) -> value.insert(row.getFields().get(value.getField()), row.getId()));
    }

    @Override
    public void delete(Row row) {
        fieldKeepers.forEach((key, value) -> value.delete(row.getFields().get(value.getField()), row.getId()));
    }
}
