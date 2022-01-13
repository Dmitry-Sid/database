package server.model.pojo;

import org.apache.commons.lang3.StringUtils;
import server.model.ConditionException;

import java.util.Objects;

public class SimpleCondition implements FieldCondition {
    private final SimpleType type;
    private final String field;
    private final Comparable value;

    private SimpleCondition(SimpleType type, String field, Comparable value) {
        this.type = type;
        this.field = field;
        this.value = value;
    }

    public static SimpleCondition make(SimpleType type, String field, Comparable value) throws ConditionException {
        check(type, field, value);
        return new SimpleCondition(type, field, value);
    }

    private static void check(SimpleType type, String field, Comparable value) throws ConditionException {
        if (value == null) {
            return;
        }
        if (StringUtils.isBlank(field)) {
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
