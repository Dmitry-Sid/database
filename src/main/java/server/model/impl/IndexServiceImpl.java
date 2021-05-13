package server.model.impl;

import server.model.*;
import server.model.pojo.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class IndexServiceImpl implements IndexService {
    private static final SearchResult EMPTY = new SearchResult(false, Collections.emptySet());
    private final Map<String, FieldKeeper> fieldKeepers;
    private final String path;
    private final ObjectConverter objectConverter;
    private final ConditionService conditionService;
    private List<Runnable> runnableList = new CopyOnWriteArrayList<>();

    public IndexServiceImpl(Map<String, FieldKeeper> fieldKeepers, ConditionService conditionService) {
        this.fieldKeepers = fieldKeepers;
        this.conditionService = conditionService;
        this.path = null;
        this.objectConverter = null;
    }

    public IndexServiceImpl(String path, ObjectConverter objectConverter, ModelService modelService, ConditionService conditionService) {
        this.path = path;
        this.objectConverter = objectConverter;
        this.conditionService = conditionService;
        this.fieldKeepers = new ConcurrentHashMap<>();
        if (new File(path).exists()) {
            final Set<String> fields = objectConverter.fromFile(HashSet.class, path);
            fields.forEach(field -> fieldKeepers.put(field, createFieldKeeper(field)));
        }
        modelService.subscribeOnIndexesChanges(fields -> {
            final AtomicBoolean added = new AtomicBoolean(false);
            fields.forEach(field -> {
                if (!fieldKeepers.containsKey(field)) {
                    added.set(true);
                    fieldKeepers.putIfAbsent(field, createFieldKeeper(field));
                }
            });
            final Set<String> fieldSet = fieldKeepers.keySet().stream().filter(field -> !fields.contains(field)).collect(Collectors.toSet());
            fieldSet.forEach(field -> {
                final FieldKeeper fieldKeeper = fieldKeepers.get(field);
                fieldKeeper.clear();
                fieldKeepers.remove(field);
            });
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
        return searchResult(condition);
    }

    private SearchResult searchResult(ICondition condition) {
        if (condition instanceof SimpleCondition) {
            final FieldKeeper fieldKeeper = fieldKeepers.get(((SimpleCondition) condition).getField());
            if (fieldKeeper != null) {
                return new SearchResult(true, fieldKeeper.search(conditionService, (SimpleCondition) condition));
            } else {
                return new SearchResult(false, null);
            }
        } else if (condition instanceof ComplexCondition) {
            return searchResult((ComplexCondition) condition);
        } else if (condition instanceof EmptyCondition) {
            return new SearchResult(true, Collections.emptySet());
        }
        throw new ConditionException("Unknown condition class : " + condition.getClass());
    }

    private SearchResult searchResult(ComplexCondition condition) {
        boolean found = true;
        Set<Integer> result = null;
        for (ICondition innerCondition : condition.getConditions()) {
            if (!found) {
                break;
            }
            if (result == null) {
                final SearchResult searchResult = searchResult(innerCondition);
                if (searchResult.found) {
                    result = searchResult.idSet;
                }
                continue;
            }
            final SearchResult searchResult = searchResult(innerCondition);
            switch (condition.getType()) {
                case OR:
                    if (!searchResult.found) {
                        found = false;
                        result = null;
                        break;
                    }
                    result.addAll(searchResult(innerCondition).idSet);
                    break;
                case AND:
                    if (searchResult.found) {
                        result.retainAll(searchResult(innerCondition).idSet);
                    }
                    break;
                default:
                    throw new ConditionException("Unknown complex type : " + condition.getType());
            }
        }
        return new SearchResult(found, result);
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

    private <U extends Comparable<U>, V> FieldKeeper<U, V> createFieldKeeper(String fieldName) {
        return new FieldMap<>(fieldName, path, objectConverter);
    }

    @Override
    public void destroy() {
        objectConverter.toFile(new HashSet<>(fieldKeepers.keySet()), path);
        fieldKeepers.values().forEach(FieldKeeper::destroy);
    }
}
