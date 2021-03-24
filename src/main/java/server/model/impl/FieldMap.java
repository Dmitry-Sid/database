package server.model.impl;

import server.model.ConditionService;
import server.model.FieldKeeper;
import server.model.pojo.SimpleCondition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FieldMap<U extends Comparable, V> extends FieldKeeper<U, V> {
    private final Map<U, Set<V>> valuesMap;

    public FieldMap(String fieldName, ConditionService conditionService, Map<U, Set<V>> valuesMap) {
        super(fieldName, conditionService);
        this.valuesMap = valuesMap;
    }

    @Override
    public void insert(U key, V value) {
        valuesMap.computeIfPresent(key, (mapKey, set) -> {
            set.add(value);
            return set;
        });
        valuesMap.putIfAbsent(key, new HashSet<>(Collections.singletonList(value)));
    }

    @Override
    public boolean delete(U key, V value) {
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
    public Set<V> search(SimpleCondition condition) {
        return valuesMap.entrySet().stream().filter(entry -> conditionService.check(entry.getKey(), condition))
                .flatMap(entry -> entry.getValue().stream()).collect(Collectors.toSet());
    }

    @Override
    public Set<V> search(U key) {
        return valuesMap.getOrDefault(key, Collections.emptySet());
    }
}
