package server.model.impl;

import server.model.BaseFieldKeeper;
import server.model.ConditionService;
import server.model.ObjectConverter;
import server.model.Utils;
import server.model.pojo.SimpleCondition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class FieldMap<U extends Comparable<U>, V> extends BaseFieldKeeper<U, V> {

    public FieldMap(String fieldName, String path, ObjectConverter objectConverter, ConditionService conditionService) {
        super(fieldName, path, objectConverter, conditionService);
    }

    @Override
    protected Variables<U, V> createVariables() {
        return new MapVariables<>();
    }

    @Override
    public void insertNotNull(U key, V value) {
        getVariables().valuesMap.computeIfPresent(key, (mapKey, set) -> {
            final int size = set.size();
            set.add(value);
            if (size != set.size()) {
                changed = true;
            }
            return set;
        });
        final int size = getVariables().valuesMap.size();
        getVariables().valuesMap.putIfAbsent(key, new HashSet<>(Collections.singletonList(value)));
        if (size != getVariables().valuesMap.size()) {
            changed = true;
        }
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
    public void conditionSearchNotNull(SimpleCondition condition, Set<V> set, int size) {
        for (Map.Entry<U, Set<V>> entry : getVariables().valuesMap.entrySet()) {
            if (conditionService.check(entry.getKey(), condition)) {
                if (Utils.fillToFull(set, size, entry.getValue())) {
                    return;
                }
            }
        }
    }

    @Override
    public Set<V> searchNotNull(U key) {
        return getVariables().valuesMap.getOrDefault(key, Collections.emptySet());
    }


    private MapVariables<U, V> getVariables() {
        return (MapVariables<U, V>) variables;
    }

    private static class MapVariables<U, V> extends Variables<U, V> {
        private static final long serialVersionUID = 674193037333230950L;
        private final Map<U, Set<V>> valuesMap = new ConcurrentHashMap<>();
    }
}
