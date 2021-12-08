package server.model.pojo;

import server.model.ConditionException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ComplexCondition implements ICondition {
    private final ComplexType type;
    private final Set<ICondition> conditions;

    private ComplexCondition(ComplexType type, Collection<ICondition> conditions) {
        this.type = type;
        this.conditions = new HashSet<>(conditions);
    }

    public static ComplexCondition make(ComplexType type, ICondition... conditions) throws ConditionException {
        if (conditions == null || conditions.length == 0) {
            throw new ConditionException("empty inner conditions");
        }
        final Set<ICondition> conditionSet = new HashSet<>();
        for (ICondition iCondition : conditions) {
            if (iCondition instanceof EmptyCondition) {
                throw new ConditionException("inner condition cannot be empty");
            }
            conditionSet.add(iCondition);
        }
        return new ComplexCondition(type, conditionSet);
    }

    public static ComplexCondition make(ComplexType type, Collection<ICondition> conditions) throws ConditionException {
        if (conditions == null || conditions.size() == 0) {
            throw new ConditionException("empty inner conditions");
        }
        for (ICondition iCondition : conditions) {
            if (iCondition instanceof EmptyCondition) {
                throw new ConditionException("inner condition cannot be empty");
            }
        }
        return new ComplexCondition(type, conditions);
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
