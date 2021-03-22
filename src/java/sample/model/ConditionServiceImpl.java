package sample.model;

import org.apache.commons.lang3.StringUtils;
import sample.model.pojo.*;

import java.util.ArrayList;
import java.util.List;

public class ConditionServiceImpl implements ConditionService {

    private final ModelService modelService;

    public ConditionServiceImpl(ModelService modelService) {
        this.modelService = modelService;
    }

    @Override
    public ICondition parse(String input) {
        if (StringUtils.isBlank(input)) {
            return ICondition.empty;
        }
        final String formatted = input.trim().toLowerCase()
                .replace("\r", "").replace("\n", "");
        if (formatted.contains(ICondition.ComplexType.AND.toString().toLowerCase()) ||
                formatted.contains(ICondition.ComplexType.OR.toString().toLowerCase())) {
            return parseComplexCondition(formatted);
        }
        return parseSimpleCondition(formatted);
    }

    private ICondition parseComplexCondition(String input) {
        final int first = input.indexOf("(");
        if (first < 0) {
            throw new ConditionException("cannot find ( for ComplexCondition, input : " + input);
        }
        final int last = input.lastIndexOf(")");
        if (last < 0) {
            throw new ConditionException("cannot find ) for ComplexCondition, input : " + input);
        }
        final ICondition.ComplexType type;
        final String typeStr = input.substring(0, first);
        try {
            type = ICondition.ComplexType.valueOf(typeStr.toUpperCase());
        } catch (Exception e) {
            throw new ConditionException("unknown ComplexType type : " + typeStr);
        }
        final List<ICondition> conditions = new ArrayList<>();
        for (String part : getMainParts(input.substring(first + 1, last).trim())) {
            conditions.add(parse(part));
        }
        return new ComplexCondition(type, conditions);
    }

    public static String[] getMainParts(String input) {
        if (!(input.contains("(") || input.contains(")"))) {
            return input.split(";");
        }
        int leftBrackets = 0;
        int rightBrackets = 0;
        final List<Integer> indexes = new ArrayList<>();
        indexes.add(0);
        final char[] chars = input.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];
            if (ch == '(') {
                leftBrackets++;
            } else if (ch == ')') {
                rightBrackets++;
            }
            if (ch == ';' && leftBrackets == rightBrackets) {
                indexes.add(i);
            }
        }
        if (leftBrackets != rightBrackets) {
            throw new ConditionException("wrong brackets number for String " + input);
        }
        indexes.add(chars.length);
        final String[] parts = new String[indexes.size() - 1];
        for (int i = 0; i < indexes.size() - 1; i++) {
            if (i == 0) {
                parts[i] = input.substring(indexes.get(i), indexes.get(i + 1));
            } else {
                parts[i] = input.substring(indexes.get(i) + 1, indexes.get(i + 1));
            }
        }
        return parts;
    }

    private SimpleCondition parseSimpleCondition(String input) {
        final String formatted = input.trim();
        final ICondition.SimpleType conditionType = getConditionSign(formatted, ICondition.SimpleType.values());
        final String[] parts = formatted.split(conditionType.toString().toLowerCase());
        if (parts.length != 2) {
            throw new ConditionException("wrong condition patter : " + input);
        }
        final Pair<String, Comparable> pair = parseValue(parts);
        return new SimpleCondition(conditionType, pair.getFirst(), pair.getSecond());
    }

    private Pair<String, Comparable> parseValue(String[] parts) {
        final String field = parts[0].trim();
        final String valueStr = parts[1].trim();
        checkFieldName(field);
        final Comparable value;
        try {
            value = modelService.getValue(field, valueStr);
        } catch (NumberFormatException e) {
            throw new ConditionException("wrong value type for field : " + field + ", value : " + valueStr);
        }
        return new Pair<>(field, value);
    }

    private void checkFieldName(String field) {
        if (!modelService.contains(field)) {
            throw new ConditionException("unknown field : " + field);
        }
    }

    private <T> T getConditionSign(String formatted, T[] values) {
        if (values instanceof ICondition.SimpleType[]) {
            if (formatted.contains(ICondition.SimpleType.GTE.toString().toLowerCase())) {
                return (T) ICondition.SimpleType.GTE;
            } else if (formatted.contains(ICondition.SimpleType.LTE.toString().toLowerCase())) {
                return (T) ICondition.SimpleType.LTE;
            }
        }
        for (T type : values) {
            final String typeStr = type.toString();
            if (formatted.contains(typeStr.toLowerCase())) {
                return type;
            }
        }
        throw new ConditionException("input String does now contains allowed condition");
    }

    @Override
    public <T> boolean check(T value, ICondition condition) {
        if (condition instanceof SimpleCondition) {
            return check(value, (SimpleCondition) condition);
        } else if (condition instanceof ComplexCondition) {
            return check(value, (ComplexCondition) condition);
        } else if (condition instanceof EmptyCondition) {
            return true;
        }
        throw new ConditionException("Unknown condition class : " + condition.getClass());
    }

    private <T> boolean check(T value, ComplexCondition condition) {
        Boolean result = null;
        for (ICondition innerCondition : condition.getConditions()) {
            if (result == null) {
                result = check(value, innerCondition);
                continue;
            }
            switch (condition.getType()) {
                case OR:
                    result = result || check(value, innerCondition);
                    break;
                case AND:
                    result = result && check(value, innerCondition);
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

    private <T> boolean check(T input, SimpleCondition condition) {
        final Comparable value;
        if (input instanceof Row) {
            final Row row = (Row) input;
            if (!row.getFields().containsKey(condition.getField())) {
                throw new ConditionException("unknown field " + condition.getField());
            }
            value = row.getFields().get(condition.getField());
        } else if (input instanceof Comparable) {
            value = (Comparable) input;
        } else {
            throw new ConditionException("unknown type if input :" + input);
        }
        if (value == null) {
            throw new ConditionException("unknown field " + condition.getField());
        }
        if (ICondition.SimpleType.LIKE.equals(condition.getType())) {
            if (condition.getValue() == null) {
                return false;
            }
            if (!(value instanceof String)) {
                throw new ConditionException("wrong type from LIKE condition " + value.getClass());
            }
            return ((String) value).contains((String) condition.getValue());
        }
        final int compareResult = value.compareTo(condition.getValue());
        switch (condition.getType()) {
            case EQ:
                return compareResult == 0;
            case NOT:
                return compareResult != 0;
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
