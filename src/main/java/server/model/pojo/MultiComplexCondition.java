package server.model.pojo;

import server.model.ConditionException;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class MultiComplexCondition extends AbstractComplexCondition<ICondition> {

    private MultiComplexCondition(ComplexType type, Set<ICondition> conditions) {
        super(type, conditions);
    }

    public static MultiComplexCondition make(ComplexType type, ICondition... conditions) throws ConditionException {
        if (conditions == null || conditions.length == 0) {
            throw new ConditionException("empty inner conditions");
        }
        final Set<ICondition> conditionSet = new LinkedHashSet<>();
        for (ICondition iCondition : conditions) {
            if (iCondition instanceof EmptyCondition) {
                throw new ConditionException("inner condition cannot be empty");
            }
            conditionSet.add(iCondition);
        }
        return new MultiComplexCondition(type, conditionSet);
    }

    public static MultiComplexCondition make(ComplexType type, Collection<ICondition> conditions) throws ConditionException {
        if (conditions == null || conditions.size() == 0) {
            throw new ConditionException("empty inner conditions");
        }
        return make(type, conditions.toArray(new ICondition[0]));
    }

    @Override
    public String toString() {
        return "ComplexCondition{" +
                "type=" + type +
                ", conditions=" + conditions +
                '}';
    }
}
