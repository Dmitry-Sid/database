package server.model.impl;

import server.model.*;
import server.model.pojo.*;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class IndexServiceImpl implements IndexService {
    private static final SearchResult EMPTY = new SearchResult(false, Collections.emptySet());
    private final Map<String, FieldKeeper> fieldKeepers;
    private final String fileName;
    private final ObjectConverter objectConverter;
    private final ConditionService conditionService;
    private List<Runnable> runnableList = new CopyOnWriteArrayList<>();

    public IndexServiceImpl(Map<String, FieldKeeper> fieldKeepers, ConditionService conditionService) {
        this.fieldKeepers = fieldKeepers;
        this.conditionService = conditionService;
        this.fileName = null;
        this.objectConverter = null;
    }

    public IndexServiceImpl(String fileName, ObjectConverter objectConverter, ModelService modelService, ConditionService conditionService) {
        this.fileName = fileName;
        this.objectConverter = objectConverter;
        this.conditionService = conditionService;
        if (new File(fileName).exists()) {
            this.fieldKeepers = objectConverter.fromFile(HashMap.class, fileName);
        } else {
            this.fieldKeepers = new HashMap<>();
        }
        modelService.subscribeOnIndexesChanges(fields -> {
            final AtomicBoolean added = new AtomicBoolean(false);
            fields.forEach(field -> {
                if (!fieldKeepers.containsKey(field)) {
                    added.set(true);
                    fieldKeepers.putIfAbsent(field, new FieldMap(field, new ConcurrentHashMap<>()));
                }
            });
            final Set<String> fieldSet = fieldKeepers.keySet().stream().filter(field -> !fields.contains(field)).collect(Collectors.toSet());
            fieldSet.forEach(fieldKeepers::remove);
            if (added.get()) {
                runnableList.forEach(Runnable::run);
            }
        });
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
            return fieldKeepers.get(((SimpleCondition) condition).getField()).search(conditionService, (SimpleCondition) condition);
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

    @Override
    public void subscribeOnIndexesChanges(Runnable runnable) {
        runnableList.add(runnable);
    }

    private void destroy() {
        objectConverter.toFile((Serializable) fieldKeepers, fileName);
    }
}
