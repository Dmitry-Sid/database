package sample.model.pojo;

import sample.model.ConditionException;

import java.util.*;

public class ComplexCondition implements ICondition {
    private final ComplexType type;
    private final Set<ICondition> conditions;

    public ComplexCondition(ComplexType type, ICondition... conditions) {
        if (conditions == null || conditions.length == 0) {
            throw new ConditionException("empty inner conditions");
        }
        this.type = type;
        this.conditions = new HashSet<>(Arrays.asList(conditions));
    }

    public ComplexCondition(ComplexType type, Collection<ICondition> conditions) {
        if (conditions == null || conditions.size() == 0) {
            throw new ConditionException("empty inner conditions");
        }
        this.type = type;
        this.conditions = new HashSet<>(conditions);
    }

    public ComplexType getType() {
        return type;
    }

    public Set<ICondition> getConditions() {
        return conditions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ComplexCondition)) {
            return false;
        }
        final ComplexCondition that = (ComplexCondition) o;
        return getType() == that.getType() &&
                Objects.equals(getConditions(), that.getConditions());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getConditions());
    }

    @Override
    public String toString() {
        return "ComplexCondition{" +
                "type=" + type +
                ", conditions=" + conditions +
                '}';
    }
}
