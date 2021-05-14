package server.model.impl;

import server.model.BaseFieldKeeper;
import server.model.ConditionService;
import server.model.ObjectConverter;
import server.model.pojo.SimpleCondition;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FieldMap<U extends Comparable<U>, V> extends BaseFieldKeeper<U, V> {

    public FieldMap(String fieldName, String path, ObjectConverter objectConverter) {
        super(fieldName, path, objectConverter);
    }

    @Override
    protected Variables<U, V> createVariables() {
        return new MapVariables<>();
    }

    @Override
    public void insertNotNull(U key, V value) {
        getVariables().valuesMap.computeIfPresent(key, (mapKey, set) -> {
            set.add(value);
            return set;
        });
        getVariables().valuesMap.putIfAbsent(key, new HashSet<>(Collections.singletonList(value)));
    }

    @Override
    public DeleteResult deleteNotNull(U key, V value) {
        final AtomicBoolean deleted = new AtomicBoolean(false);
        final AtomicBoolean deletedFully = new AtomicBoolean(false);
        getVariables().valuesMap.computeIfPresent(key, (mapKey, set) -> {
            if (!set.contains(value)) {
                return set;
            }
            set.remove(value);
            deleted.set(true);
            if (set.isEmpty()) {
                deletedFully.set(true);
                return null;
            }
            return set;
        });
        return new DeleteResult(deleted.get(), deletedFully.get());
    }

    @Override
    public Set<V> searchNotNull(ConditionService conditionService, SimpleCondition condition) {
        return getVariables().valuesMap.entrySet().stream().filter(entry -> conditionService.check(entry.getKey(), condition))
                .flatMap(entry -> entry.getValue().stream()).collect(Collectors.toSet());
    }

    @Override
    public Set<V> searchNotNull(U key) {
        return getVariables().valuesMap.getOrDefault(key, Collections.emptySet());
    }

    @Override
    public void destroy() {
        objectConverter.toFile((Serializable) getVariables().valuesMap, getFileName());
    }

    private MapVariables<U, V> getVariables() {
        return (MapVariables<U, V>) variables;
    }

    private static class MapVariables<U, V> extends Variables<U, V> {
        private static final long serialVersionUID = 674193037333230950L;
        private final Map<U, Set<V>> valuesMap = new ConcurrentHashMap<>();
    }
}
