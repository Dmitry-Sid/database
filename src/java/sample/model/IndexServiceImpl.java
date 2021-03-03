package sample.model;

import sample.model.pojo.EmptyCondition;
import sample.model.pojo.ICondition;
import sample.model.pojo.Row;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexServiceImpl implements IndexService {
    private final ConditionService conditionService;
    private final List<FieldTree<Set<Integer>>> treeList;

    public IndexServiceImpl(ConditionService conditionService, List<FieldTree<Set<Integer>>> treeList) {
        this.conditionService = conditionService;
        this.treeList = treeList;
    }

    @Override
    public SearchResult search(ICondition iCondition) {
        final Set<Integer> idSet = new HashSet<>();
        final AtomicBoolean found = new AtomicBoolean();
        treeList.forEach(tree -> {
            final ICondition condition = conditionService.getFieldCondition(iCondition, tree.getField());
            if (condition != null && !(condition instanceof EmptyCondition)) {
                found.set(true);
                idSet.addAll(tree.search(condition).value);
            }
        });
        return new SearchResult(found.get(), idSet);
    }

    @Override
    public void transform(Row oldRow, Row row) {
        treeList.forEach(tree -> tree.transform(oldRow.getFields().get(tree.getField()),
                row.getFields().get(tree.getField()), row.getId()));
    }

    @Override
    public void insert(Row row) {
        treeList.forEach(tree -> tree.insert(row.getFields().get(tree.getField()), row.getId()));
    }

    @Override
    public void delete(Row row) {
        treeList.forEach(tree -> tree.delete(row.getFields().get(tree.getField()), row.getId()));
    }
}
