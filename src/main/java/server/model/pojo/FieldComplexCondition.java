package server.model.pojo;

import server.model.ConditionException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class FieldComplexCondition extends AbstractComplexCondition<SimpleCondition> implements FieldCondition {
    private final String field;

    private FieldComplexCondition(String field, ComplexType type, Set<SimpleCondition> conditions) {
        super(type, conditions);
        this.field = field;
    }

    public static FieldComplexCondition make(ComplexType type, ICondition... conditions) throws ConditionException {
        if (conditions == null || conditions.length == 0) {
            throw new ConditionException("empty inner conditions");
        }
        final Set<SimpleCondition> conditionSet = new HashSet<>();
        String field = null;
        for (ICondition condition : conditions) {
            if (!(condition instanceof SimpleCondition)) {
                throw new ConditionException("condition must be SimpleCondition");
            }
            final SimpleCondition simpleCondition = (SimpleCondition) condition;
            if (field == null) {
                field = simpleCondition.getField();
            } else if (!simpleCondition.getField().equals(field)) {
                throw new ConditionException("all conditions must have the same field");
            }
            conditionSet.add(simpleCondition);
        }
        return new FieldComplexCondition(field, type, conditionSet);
    }

    public static FieldComplexCondition make(ComplexType type, Collection<ICondition> conditions) throws ConditionException {
        if (conditions == null || conditions.size() == 0) {
            throw new ConditionException("empty inner conditions");
        }
        return make(type, conditions.toArray(new ICondition[0]));
    }

    public String getField() {
        return field;
    }

    public Set<SimpleCondition> getConditions() {
        return conditions;
    }

    @Override
    public ComplexType getType() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FieldComplexCondition)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final FieldComplexCondition that = (FieldComplexCondition) o;
        return Objects.equals(getField(), that.getField());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getField());
    }

    @Override
    public String toString() {
        return "FieldConditionImpl{" +
                "field='" + field + '\'' +
                ", type=" + type +
                ", conditions=" + conditions +
                '}';
    }
}