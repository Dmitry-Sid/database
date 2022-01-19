package server.model.impl;

import server.model.*;
import server.model.pojo.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class IndexServiceImpl extends BaseDestroyable implements IndexService {
    private static final String DIRECTORY = "index";
    private static final SearchResult EMPTY = new SearchResult(false, Collections.emptySet());
    private final Map<String, FieldKeeper> fieldKeepers;
    private final String path;
    private final ConditionService conditionService;
    private final List<Consumer<Set<String>>> newIndexesSubscribers = new CopyOnWriteArrayList<>();
    private volatile boolean changed;

    /**
     * Для тестов
     *
     * @param fieldKeepers
     * @param conditionService
     */
    public IndexServiceImpl(Map<String, FieldKeeper> fieldKeepers, ConditionService conditionService) {
        super(null, false, null, null);
        this.fieldKeepers = fieldKeepers;
        this.conditionService = conditionService;
        this.path = null;
    }

    public IndexServiceImpl(String filePath, boolean init, ObjectConverter objectConverter, DestroyService destroyService, ModelService modelService, ConditionService conditionService) {
        super(filePath, init, objectConverter, destroyService, Utils.getFullPath(filePath, DIRECTORY));
        this.path = Utils.getFullPath(filePath, DIRECTORY);
        this.conditionService = conditionService;
        this.fieldKeepers = new ConcurrentHashMap<>();
        if (new File(getFullPath()).exists()) {
            final Set<String> fields = objectConverter.fromFile(HashSet.class, getFullPath());
            fields.forEach(field -> fieldKeepers.put(field, createFieldKeeper(field)));
        }
        modelService.subscribeOnIndexesChanges(fields -> {
            final Set<String> newIndexes = new HashSet<>();
            fields.forEach(field -> {
                if (!fieldKeepers.containsKey(field)) {
                    changed = true;
                    newIndexes.add(field);
                    fieldKeepers.putIfAbsent(field, createFieldKeeper(field));
                }
            });
            final Set<String> fieldSet = fieldKeepers.keySet().stream().filter(field -> !fields.contains(field)).collect(Collectors.toSet());
            fieldSet.forEach(field -> {
                final FieldKeeper fieldKeeper = fieldKeepers.get(field);
                fieldKeeper.clear();
                fieldKeepers.remove(field);
                changed = true;
            });
            if (!newIndexes.isEmpty()) {
                newIndexesSubscribers.forEach(consumer -> consumer.accept(newIndexes));
            }
        });
    }

    private static String getFullPath(String path) {
        return path + "indexes";
    }

    private String getFullPath() {
        return getFullPath(path);
    }

    @Override
    public SearchResult search(ICondition condition, int size) {
        if (condition == null || condition instanceof EmptyCondition) {
            return EMPTY;
        }
        return searchResult(condition, size);
    }

    private SearchResult searchResult(ICondition condition, int size) {
        if (condition instanceof FieldCondition) {
            final FieldKeeper fieldKeeper = fieldKeepers.get(((FieldCondition) condition).getField());
            if (fieldKeeper != null) {
                return new SearchResult(true, fieldKeeper.conditionSearch((FieldCondition) condition, size));
            } else {
                return new SearchResult(false, null);
            }
        } else if (condition instanceof ComplexCondition) {
            return searchResult((ComplexCondition) condition, size);
        } else if (condition instanceof EmptyCondition) {
            return new SearchResult(true, Collections.emptySet());
        }
        throw new IllegalArgumentException("Unknown condition class : " + condition.getClass());
    }

    private SearchResult searchResult(ComplexCondition<ICondition> condition, int size) {
        SearchResult searchResult = null;
        boolean isFirst = true;
        for (ICondition innerCondition : condition.getConditions()) {
            SearchResult finalSearchResult = searchResult;
            if (Utils.checkStopConditions(isFirst, condition.getType(), finalSearchResult, r -> {
                if (ICondition.ComplexType.AND == condition.getType()) {
                    return r.idSet.isEmpty();
                }
                return !(size >= 0 && r.idSet.size() >= size);
            })) {
                break;
            }
            final SearchResult searchResultInner = searchResult(innerCondition, ICondition.ComplexType.OR == condition.getType() ? size : -1);
            if (!searchResultInner.found) {
                return new SearchResult(false, null);
            }
            if (isFirst) {
                searchResult = searchResultInner;
                isFirst = false;
                continue;
            }
            switch (condition.getType()) {
                case OR:
                    if (Utils.fillToFull(searchResult.idSet, size, searchResultInner.idSet)) {
                        return searchResult;
                    }
                    break;
                case AND:
                    searchResult.idSet.retainAll(searchResultInner.idSet);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown complex type : " + condition.getType());
            }
        }
        if (searchResult == null) {
            return new SearchResult(false, null);
        }
        return new SearchResult(true, size > -1 ? new HashSet<>(new ArrayList<>(searchResult.idSet).subList(0, Math.min(size, searchResult.idSet.size()))) : searchResult.idSet);
    }

    @Override
    public void transform(Row oldRow, Row row) {
        fieldKeepers.forEach((key, value) -> value.transform(oldRow.getFields().get(value.getFieldName()),
                row.getFields().get(value.getFieldName()), row.getId()));
    }

    @Override
    public void insert(Row row) {
        fieldKeepers.forEach((key, value) -> insert(value, row));
    }

    private void insert(FieldKeeper fieldKeeper, Row row) {
        fieldKeeper.insert(row.getFields().get(fieldKeeper.getFieldName()), row.getId());
    }

    @Override
    public void insert(Row row, Set<String> fields) {
        fields.forEach(field -> insert(fieldKeepers.get(field), row));
    }

    @Override
    public void delete(Row row) {
        fieldKeepers.forEach((key, value) -> value.delete(row.getFields().get(value.getFieldName()), row.getId()));
    }

    @Override
    public void subscribeOnNewIndexes(Consumer<Set<String>> fieldsConsumer) {
        newIndexesSubscribers.add(fieldsConsumer);
    }

    private <U extends Comparable<U>, V> FieldKeeper<U, V> createFieldKeeper(String fieldName) {
        return new BPlusTree<>(fieldName, path, objectConverter, conditionService, 2000, 1_000_000);
    }

    @Override
    public void destroy() {
        if (changed) {
            objectConverter.toFile(new HashSet<>(fieldKeepers.keySet()), getFullPath());
            changed = false;
        }
        fieldKeepers.values().forEach(FieldKeeper::destroy);
    }
}
