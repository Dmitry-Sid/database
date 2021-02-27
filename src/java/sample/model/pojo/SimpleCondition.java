package sample.model.pojo;

import sample.model.ConditionException;

import java.util.Objects;

public class SimpleCondition implements ICondition {
    private final SimpleType type;
    private final String field;
    private final Comparable value;

    public SimpleCondition(SimpleType type, String field, Comparable value) {
        check(type, field, value);
        this.type = type;
        this.field = field;
        this.value = value;
    }

    private void check(SimpleType type, String field, Comparable value) {
        if (field == null || field.trim().isEmpty()) {
            throw new ConditionException("empty field name");
        }
        if (!(value instanceof String) && SimpleType.LIKE.equals(type)) {
            throw new ConditionException("incompatible types: class " + value.getClass() + ", type " + type);
        }
    }

    public SimpleType getType() {
        return type;
    }

    public String getField() {
        return field;
    }

    public Comparable getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SimpleCondition)) {
            return false;
        }
        final SimpleCondition simpleCondition = (SimpleCondition) o;
        return type == simpleCondition.type &&
                Objects.equals(value, simpleCondition.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public String toString() {
        return "SimpleCondition{" +
                "type=" + type +
                ", field='" + field + '\'' +
                ", value=" + value +
                '}';
    }
}
