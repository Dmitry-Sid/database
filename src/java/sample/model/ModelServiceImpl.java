package sample.model;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ModelServiceImpl implements ModelService {
    private final List<Consumer<Set<String>>> fieldsChangesSubscribers = new CopyOnWriteArrayList<>();
    private final List<Consumer<Set<String>>> indexesChangesSubscribers = new CopyOnWriteArrayList<>();
    private final Map<String, FieldInfo> fields;

    public ModelServiceImpl() {
        fields = new ConcurrentHashMap<>();
    }

    private void checkFields(Map<String, FieldInfo> fields) {
        fields.forEach((key, info) -> {
            if (!types.contains(info.type)) {
                throw new RuntimeException("unknown type " + info.type);
            }
        });
    }

    @Override
    public boolean contains(String field) {
        return fields.containsKey(field);
    }

    @Override
    public Comparable getValue(String field, String value) {
        final Class<?> type = fields.get(field).type;
        if (type == null) {
            throw new RuntimeException("unknown field " + field);
        }
        if (Byte.class.equals(type)) {
            return Byte.parseByte(value);
        } else if (Character.class.equals(type)) {
            if (StringUtils.isBlank(value) || value.length() > 1) {
                throw new RuntimeException("wrong format for " + field + ", expected Character but was " + value);
            }
            return value.charAt(0);
        } else if (Short.class.equals(type)) {
            return Short.parseShort(value);
        } else if (Integer.class.equals(type)) {
            return Integer.parseInt(value);
        } else if (Long.class.equals(type)) {
            return Long.parseLong(value);
        } else if (Float.class.equals(type)) {
            return Float.parseFloat(value);
        } else if (Double.class.equals(type)) {
            return Double.parseDouble(value);
        }
        return value;
    }

    @Override
    public void add(String field, Class<?> type) {
        if (!types.contains(type)) {
            throw new RuntimeException("unknown type " + type);
        }
        fields.putIfAbsent(field, new FieldInfo(type, new AtomicBoolean(false)));
        fieldsChangesSubscribers.forEach(consumer -> consumer.accept(getFields()));
    }

    @Override
    public void delete(String field) {
        fields.remove(field);
        fieldsChangesSubscribers.forEach(consumer -> consumer.accept(getFields()));
    }

    @Override
    public void addIndex(String field) {
        fields.computeIfPresent(field, (key, info) -> {
            info.isIndex.set(true);
            return info;
        });
        indexesChangesSubscribers.forEach(consumer -> consumer.accept(getIndexedFields()));
    }

    @Override
    public void deleteIndex(String field) {
        fields.computeIfPresent(field, (key, info) -> {
            info.isIndex.set(false);
            return info;
        });
        indexesChangesSubscribers.forEach(consumer -> consumer.accept(getIndexedFields()));
    }

    @Override
    public Set<String> getFields() {
        return fields.keySet();
    }

    @Override
    public Set<String> getIndexedFields() {
        return fields.entrySet().stream().filter(entry -> entry.getValue().isIndex.get()).map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    @Override
    public void subscribeOnFieldsChanges(Consumer<Set<String>> fieldsConsumer) {
        fieldsChangesSubscribers.add(fieldsConsumer);
    }

    @Override
    public void subscribeOnIndexesChanges(Consumer<Set<String>> fieldsConsumer) {
        indexesChangesSubscribers.add(fieldsConsumer);
    }

    private static class FieldInfo {
        private final Class<?> type;
        private final AtomicBoolean isIndex;

        private FieldInfo(Class<?> type, AtomicBoolean isIndex) {
            this.type = type;
            this.isIndex = isIndex;
        }
    }
}
