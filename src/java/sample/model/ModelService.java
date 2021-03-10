package sample.model;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public interface ModelService {
    public static final List<Class<?>> types = Arrays.asList(Byte.class, Character.class, Short.class, Integer.class,
            Long.class, Float.class, Double.class, String.class);

    public boolean contains(String field);

    public Comparable getValue(String field, String value);

    public void add(String field, Class<?> type);

    public void delete(String field);

    public void addIndex(String field);

    public void deleteIndex(String field);

    public Set<String> getFields();

    public Set<String> getIndexedFields();

    public void subscribeOnFieldsChanges(Consumer<Set<String>> fieldsConsumer);

    public void subscribeOnIndexesChanges(Consumer<Set<String>> fieldsConsumer);

}
