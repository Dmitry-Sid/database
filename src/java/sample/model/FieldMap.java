package sample.model;

import sample.model.pojo.SimpleCondition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    public void delete(U key, V value) {
        valuesMap.computeIfPresent(key, (mapKey, set) -> {
            set.remove(value);
            if (set.isEmpty()) {
                return null;
            }
            return set;
        });
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
