package server.model;

import server.model.pojo.FieldCondition;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public abstract class BaseFieldKeeper<U extends Comparable<U>, V> implements FieldKeeper<U, V>, Destroyable {
    protected static DeleteResult NOT = new DeleteResult(false, false);
    protected static DeleteResult NOT_FULLY = new DeleteResult(true, false);
    protected static DeleteResult FULLY = new DeleteResult(true, true);

    protected final String fieldName;
    protected final String path;
    protected final ObjectConverter objectConverter;
    protected final ConditionService conditionService;
    protected final Variables<U, V> variables;
    protected volatile boolean changed;

    protected BaseFieldKeeper(String fieldName, String path, ObjectConverter objectConverter, ConditionService conditionService) {
        this.fieldName = fieldName;
        this.path = path;
        this.objectConverter = objectConverter;
        this.conditionService = conditionService;
        if (new File(getFileName()).exists()) {
            this.variables = objectConverter.fromFile(Variables.class, getFileName());
        } else {
            this.variables = createVariables();
        }
    }

    protected abstract Variables<U, V> createVariables();

    @Override
    public void transform(U oldKey, U key, V value) {
        if (oldKey == null && key == null) {
            return;
        }
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
            final int size = variables.nullSet.size();
            variables.nullSet.add(value);
            if (size != variables.nullSet.size()) {
                changed = true;
            }
            return;
        }
        insertNotNull(key, value);
    }

    @Override
    public DeleteResult delete(U key, V value) {
        final DeleteResult deleteResult;
        if (key == null) {
            deleteResult = new DeleteResult(variables.nullSet.remove(value), variables.nullSet.isEmpty());
        } else {
            deleteResult = deleteNotNull(key, value);
        }
        if (deleteResult.deleted) {
            changed = true;
        }
        return deleteResult;
    }

    @Override
    public Set<V> conditionSearch(FieldCondition condition, int size) {
        final Set<V> set = new HashSet<>();
        conditionSearchNotNull(condition, set, size);
        if (!Utils.isFull(set, size) && conditionService.check(null, condition)) {
            Utils.fillToFull(set, size, variables.nullSet);
        }
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

    protected abstract void conditionSearchNotNull(FieldCondition condition, Set<V> set, int size);

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

    @Override
    public void destroy() {
        if (changed) {
            objectConverter.toFile(variables, getFileName());
            changed = false;
        }
    }

    protected abstract static class Variables<U, V> implements Serializable {
        final Set<V> nullSet = new CopyOnWriteArraySet<>();
    }
}
