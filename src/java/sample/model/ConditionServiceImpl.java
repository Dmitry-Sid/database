package sample.model;

import sample.model.pojo.ComplexCondition;
import sample.model.pojo.ICondition;
import sample.model.pojo.Row;
import sample.model.pojo.SimpleCondition;

public class ConditionServiceImpl implements ConditionService {
    @Override
    public ICondition parse(String input) {
        return null;
    }

    @Override
    public boolean check(Row row, ICondition condition) {
        if (condition instanceof SimpleCondition) {
            return check(row, (SimpleCondition) condition);
        } else if (condition instanceof ComplexCondition) {
            return check(row, (ComplexCondition) condition);
        }
        throw new ConditionException("Unknown condition class : " + condition.getClass());
    }

    private boolean check(Row row, ComplexCondition condition) {
        Boolean result = null;
        for (ICondition innerCondition : condition.getConditions()) {
            if (result == null) {
                result = check(row, innerCondition);
                continue;
            }
            switch (condition.getType()) {
                case OR:
                    result = result || check(row, innerCondition);
                    break;
                case AND:
                    result = result && check(row, innerCondition);
                    break;
                default:
                    throw new ConditionException("Unknown complex type : " + condition.getType());
            }
        }
        if (result == null) {
            return true;
        }
        return result;
    }

    private boolean check(Row row, SimpleCondition condition) {
        if (!row.getFields().containsKey(condition.getField())) {
            throw new ConditionException("unknown field " + condition.getField());
        }
        final Comparable value = row.getFields().get(condition.getField());
        if (value == null) {
            return condition.getValue() == null;
        }
        if (ICondition.SimpleType.LIKE.equals(condition.getType())) {
            if (condition.getValue() == null) {
                return false;
            }
            return ((String) value).contains((String) condition.getValue());
        }
        final int compareResult = value.compareTo(condition.getValue());
        switch (condition.getType()) {
            case EQ:
                return compareResult == 0;
            case GT:
                return compareResult > 0;
            case LT:
                return compareResult < 0;
            case GTE:
                return compareResult >= 0;
            case LTE:
                return compareResult <= 0;
            default:
                throw new ConditionException("Unknown simple type : " + condition.getType());
        }
    }

}
