package server.model.impl;

import server.model.ConditionService;
import server.model.FieldKeeper;
import server.model.ObjectConverter;
import server.model.pojo.SimpleCondition;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FieldMap<U extends Comparable, V> extends FieldKeeper<U, V> {
    private final Map<U, Set<V>> valuesMap;
    private final String fileName;
    private final ObjectConverter objectConverter;

    public FieldMap(String fieldName, String fileName, ObjectConverter objectConverter) {
        super(fieldName);
        this.fileName = fileName;
        this.objectConverter = objectConverter;
        if (new File(fileName).exists()) {
            this.valuesMap = objectConverter.fromFile(ConcurrentHashMap.class, fileName);
        } else {
            this.valuesMap = new ConcurrentHashMap<>();
        }
    }

    @Override
    public void insert(U key, V value) {
        if (key == null) {
            return;
        }
        valuesMap.computeIfPresent(key, (mapKey, set) -> {
            set.add(value);
            return set;
        });
        valuesMap.putIfAbsent(key, new HashSet<>(Collections.singletonList(value)));
    }

    @Override
    public boolean delete(U key, V value) {
        if (key == null) {
            return true;
        }
        final AtomicBoolean deleted = new AtomicBoolean(false);
        valuesMap.computeIfPresent(key, (mapKey, set) -> {
            if (!set.contains(value)) {
                deleted.set(false);
                return set;
            }
            set.remove(value);
            deleted.set(true);
            if (set.isEmpty()) {
                return null;
            }
            return set;
        });
        return deleted.get();
    }

    @Override
    public Set<V> search(ConditionService conditionService, SimpleCondition condition) {
        return valuesMap.entrySet().stream().filter(entry -> conditionService.check(entry.getKey(), condition))
                .flatMap(entry -> entry.getValue().stream()).collect(Collectors.toSet());
    }

    @Override
    public Set<V> search(U key) {
        return valuesMap.getOrDefault(key, Collections.emptySet());
    }

    @Override
    public void destroy() {
        objectConverter.toFile((Serializable) valuesMap, fileName);
    }
}
