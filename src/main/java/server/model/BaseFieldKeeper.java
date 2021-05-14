package server.model;

import server.model.pojo.SimpleCondition;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public abstract class BaseFieldKeeper<U extends Comparable<U>, V> implements FieldKeeper<U, V> {
    protected static DeleteResult NOT = new DeleteResult(false, false);
    protected static DeleteResult NOT_FULLY = new DeleteResult(true, false);
    protected static DeleteResult FULLY = new DeleteResult(true, true);

    protected final String fieldName;
    protected final String path;
    protected final ObjectConverter objectConverter;
    protected final Variables<U, V> variables;

    protected BaseFieldKeeper(String fieldName, String path, ObjectConverter objectConverter) {
        this.fieldName = fieldName;
        this.path = path;
        this.objectConverter = objectConverter;
        if (new File(getFileName()).exists()) {
            this.variables = objectConverter.fromFile(Variables.class, getFileName());
        } else {
            this.variables = createVariables();
        }
    }

    protected abstract Variables<U, V> createVariables();

    @Override
    public void transform(U oldKey, U key, V value) {
        if (oldKey != null && oldKey.equals(key)) {
            return;
        }
        if (delete(oldKey, value).deleted) {
            insert(key, value);
        }
    }

    @Override
    public void insert(U key, V value) {
        if (key == null) {
            variables.nullSet.add(value);
            return;
        }
        insertNotNull(key, value);
    }

    @Override
    public DeleteResult delete(U key, V value) {
        if (key == null) {
            return new DeleteResult(variables.nullSet.remove(value), variables.nullSet.isEmpty());
        }
        return deleteNotNull(key, value);
    }

    @Override
    public Set<V> search(ConditionService conditionService, SimpleCondition condition) {
        final Set<V> set = searchNotNull(conditionService, condition);
        variables.nullSet.stream().filter(value -> conditionService.check(value, condition)).forEach(set::add);
        return set;
    }

    @Override
    public Set<V> search(U key) {
        if (key == null) {
            return new HashSet<>(variables.nullSet);
        }
        return searchNotNull(key);
    }

    protected abstract void insertNotNull(U key, V value);

    protected abstract DeleteResult deleteNotNull(U key, V value);

    protected abstract Set<V> searchNotNull(ConditionService conditionService, SimpleCondition condition);

    protected abstract Set<V> searchNotNull(U key);

    public String getFieldName() {
        return fieldName;
    }

    public void clear() {
        new File(getFileName()).delete();
    }

    protected String getFileName() {
        return path + "." + fieldName;
    }

    protected String getFileName(Object object) {
        return path + object + "." + fieldName;
    }

    protected abstract static class Variables<U, V> implements Serializable {
        final Set<V> nullSet = new CopyOnWriteArraySet<>();
    }
}
