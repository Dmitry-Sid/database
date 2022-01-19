package server.model.impl;

import org.apache.commons.lang3.StringUtils;
import server.model.ConditionException;
import server.model.ConditionService;
import server.model.ModelService;
import server.model.Utils;
import server.model.pojo.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static server.model.pojo.ICondition.SimpleType.*;

public class ConditionServiceImpl implements ConditionService {

    private final ModelService modelService;

    public ConditionServiceImpl(ModelService modelService) {
        this.modelService = modelService;
    }

    private static String[] getMainParts(String input) throws ConditionException {
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

    @Override
    public ICondition parse(String input) throws ConditionException {
        if (StringUtils.isBlank(input)) {
            return ICondition.empty;
        }
        final String formatted = input.trim().replace("\r", "").replace("\n", "");
        if (formatted.contains(ICondition.ComplexType.AND.toString()) ||
                formatted.contains(ICondition.ComplexType.OR.toString())) {
            return transform(parseComplexCondition(formatted));
        }
        return parseSimpleCondition(formatted);
    }

    private ICondition transform(ComplexCondition<ICondition> complexCondition) throws ConditionException {
        final ICondition.ComplexType complexType = complexCondition.getType();
        final Map<String, Collection<SimpleCondition>> conditionMap = new HashMap<>();
        if (!checkAndFillMap(complexCondition, complexType, conditionMap)) {
            return complexCondition;
        }
        final Set<ICondition> fieldsConditions = new HashSet<>();
        for (Collection<SimpleCondition> conditions : conditionMap.values()) {
            final ConditionMemory conditionMemory;
            switch (complexType) {
                case AND:
                    conditionMemory = new AndConditionMemory();
                    break;
                case OR:
                    conditionMemory = new OrConditionMemory();
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + complexType);
            }
            for (SimpleCondition condition : sort(conditions)) {
                switch (complexType) {
                    case AND: {
                        final AndConditionMemory andConditionMemory = (AndConditionMemory) conditionMemory;
                        switch (condition.getType()) {
                            case EQ: {
                                checkAndRemove(condition, andConditionMemory.lt, conditionMemory.fieldConditions, v -> v < 0, "EQ condition cannot have greater or equal value than LT value");
                                checkAndRemove(condition, andConditionMemory.lte, conditionMemory.fieldConditions, v -> v <= 0, "EQ condition cannot have greater value than LTE value");
                                checkAndRemove(condition, andConditionMemory.gt, conditionMemory.fieldConditions, v -> v > 0, "EQ condition cannot have lower or equal value than GT value");
                                checkAndRemove(condition, andConditionMemory.gte, conditionMemory.fieldConditions, v -> v >= 0, "EQ condition cannot have lower value than GTE value");
                                for (SimpleCondition not : andConditionMemory.notSet) {
                                    checkAndRemove(condition, not, conditionMemory.fieldConditions, v -> v != 0, "EQ condition cannot have same value as NOT value");
                                }
                                if (andConditionMemory.like != null) {
                                    final String s = (String) condition.getValue();
                                    if (s.contains((String) andConditionMemory.like.getValue())) {
                                        conditionMemory.fieldConditions.remove(andConditionMemory.like);
                                    } else {
                                        throw makeConditionException("EQ condition cannot have other value than LIKE value", condition, andConditionMemory.like);
                                    }
                                }
                                if (andConditionMemory.eq == null || condition.getValue().compareTo(andConditionMemory.eq.getValue()) == 0) {
                                    andConditionMemory.eq = condition;
                                } else {
                                    throw makeConditionException("cannot be more than one EQ condition inside AND condition", condition, andConditionMemory.eq);
                                }
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                            case LT: {
                                final Runnable addRunnable = () -> substitute(condition, andConditionMemory.lt, conditionMemory.fieldConditions, v -> v < 0, andConditionMemory::setLt);
                                if (andConditionMemory.lte != null) {
                                    if (condition.getValue().compareTo(andConditionMemory.lte.getValue()) <= 0) {
                                        conditionMemory.fieldConditions.remove(andConditionMemory.lte);
                                        addRunnable.run();
                                    }
                                    continue;
                                }
                                check(condition, andConditionMemory.gt, v -> v > 0, "LT condition cannot have lower or equal value than GT value");
                                check(condition, andConditionMemory.gte, v -> v > 0, "LT condition cannot have lower or equal value than GTE value");
                                for (SimpleCondition not : andConditionMemory.notSet) {
                                    checkAndRemove(condition, not, conditionMemory.fieldConditions, v -> v <= 0, null);
                                }
                                addRunnable.run();
                                break;
                            }
                            case LTE: {
                                check(condition, andConditionMemory.gt, v -> v > 0, "LTE condition cannot have lower or equal value than GT value");
                                check(condition, andConditionMemory.gte, v -> v >= 0, "LTE condition cannot have lower value than GTE value");
                                for (SimpleCondition not : andConditionMemory.notSet) {
                                    checkAndRemove(condition, not, conditionMemory.fieldConditions, v -> v < 0, null);
                                }
                                substitute(condition, andConditionMemory.lte, conditionMemory.fieldConditions, v -> v < 0, andConditionMemory::setLte);
                                break;
                            }
                            case GT: {
                                final Runnable addRunnable = () -> substitute(condition, andConditionMemory.gt, conditionMemory.fieldConditions, v -> v > 0, andConditionMemory::setGt);
                                if (andConditionMemory.gte != null) {
                                    if (condition.getValue().compareTo(andConditionMemory.gte.getValue()) >= 0) {
                                        conditionMemory.fieldConditions.remove(andConditionMemory.gte);
                                        addRunnable.run();
                                    }
                                    continue;
                                }
                                for (SimpleCondition not : andConditionMemory.notSet) {
                                    checkAndRemove(condition, not, conditionMemory.fieldConditions, v -> v >= 0, null);
                                }
                                addRunnable.run();
                                break;
                            }
                            case GTE: {
                                for (SimpleCondition not : andConditionMemory.notSet) {
                                    checkAndRemove(condition, not, conditionMemory.fieldConditions, v -> v > 0, null);
                                }
                                substitute(condition, andConditionMemory.gte, conditionMemory.fieldConditions, v -> v > 0, andConditionMemory::setGte);
                                break;
                            }
                            case LIKE: {
                                if (andConditionMemory.like != null) {
                                    final String s1 = (String) condition.getValue();
                                    final String s2 = (String) andConditionMemory.like.getValue();
                                    if (s1.contains(s2)) {
                                        if (s1.length() > s2.length()) {
                                            conditionMemory.fieldConditions.remove(andConditionMemory.like);
                                        } else {
                                            continue;
                                        }
                                    } else if (s2.contains(s1)) {
                                        continue;
                                    } else {
                                        throw makeConditionException("LIKE condition cannot have other value than LIKE value", condition, andConditionMemory.like);
                                    }
                                }
                                for (final SimpleCondition not : andConditionMemory.notSet) {
                                    final String s1 = (String) condition.getValue();
                                    final String s2 = (String) not.getValue();
                                    if (s1.contains(s2) && s1.length() > s2.length()) {
                                        conditionMemory.fieldConditions.remove(not);
                                    }
                                }
                                andConditionMemory.like = condition;
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                            case NOT: {
                                andConditionMemory.notSet.add(condition);
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                        }
                        break;
                    }
                    case OR: {
                        final OrConditionMemory orConditionMemory = (OrConditionMemory) conditionMemory;
                        switch (condition.getType()) {
                            case EQ: {
                                if (!check(condition, orConditionMemory.lt, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (!check(condition, orConditionMemory.lte, v -> v > 0, null)) {
                                    continue;
                                }
                                if (!check(condition, orConditionMemory.gt, v -> v <= 0, null)) {
                                    continue;
                                }
                                if (!check(condition, orConditionMemory.gte, v -> v < 0, null)) {
                                    continue;
                                }
                                if (orConditionMemory.not != null) {
                                    if (condition.getValue().compareTo(orConditionMemory.not.getValue()) == 0) {
                                        conditionMemory.fieldConditions.remove(orConditionMemory.not);
                                    }
                                    continue;
                                }
                                boolean contains = false;
                                for (SimpleCondition like : orConditionMemory.likeSet) {
                                    final String s = (String) condition.getValue();
                                    if (s.contains((String) like.getValue())) {
                                        contains = true;
                                        break;
                                    }
                                }
                                if (contains) {
                                    continue;
                                }
                                orConditionMemory.eqSet.add(condition);
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                            case LT: {
                                if (orConditionMemory.lte != null && !checkAndRemove(condition, orConditionMemory.lte, conditionMemory.fieldConditions, v -> v > 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, orConditionMemory.gt, conditionMemory.fieldConditions, v -> v > 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, orConditionMemory.gte, conditionMemory.fieldConditions, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (orConditionMemory.not != null) {
                                    checkAndRemove(condition, orConditionMemory.not, conditionMemory.fieldConditions, v -> v > 0, null);
                                    continue;
                                }
                                if (orConditionMemory.lt != null && !checkAndRemove(condition, orConditionMemory.lt, conditionMemory.fieldConditions, v -> v > 0, null)) {
                                    continue;
                                }
                                orConditionMemory.lt = condition;
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                            case LTE: {
                                if (checkAndRemove(condition, orConditionMemory.gt, conditionMemory.fieldConditions, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, orConditionMemory.gte, conditionMemory.fieldConditions, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (orConditionMemory.not != null) {
                                    checkAndRemove(condition, orConditionMemory.not, conditionMemory.fieldConditions, v -> v >= 0, null);
                                    continue;
                                }
                                if (orConditionMemory.lte != null && !checkAndRemove(condition, orConditionMemory.lte, conditionMemory.fieldConditions, v -> v > 0, null)) {
                                    continue;
                                }
                                orConditionMemory.lte = condition;
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                            case GT: {
                                if (orConditionMemory.gte != null && !checkAndRemove(condition, orConditionMemory.gte, conditionMemory.fieldConditions, v -> v < 0, null)) {
                                    continue;
                                }
                                if (orConditionMemory.not != null) {
                                    checkAndRemove(condition, orConditionMemory.not, conditionMemory.fieldConditions, v -> v < 0, null);
                                    continue;
                                }
                                if (orConditionMemory.gt != null && !checkAndRemove(condition, orConditionMemory.gt, conditionMemory.fieldConditions, v -> v < 0, null)) {
                                    continue;
                                }
                                orConditionMemory.gt = condition;
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                            case GTE: {
                                if (orConditionMemory.not != null) {
                                    checkAndRemove(condition, orConditionMemory.not, conditionMemory.fieldConditions, v -> v <= 0, null);
                                    continue;
                                }
                                if (orConditionMemory.gte != null && !checkAndRemove(condition, orConditionMemory.gte, conditionMemory.fieldConditions, v -> v < 0, null)) {
                                    continue;
                                }
                                orConditionMemory.gte = condition;
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                            case LIKE: {
                                boolean skip = false;
                                for (final SimpleCondition like : orConditionMemory.likeSet) {
                                    final String s1 = (String) condition.getValue();
                                    final String s2 = (String) like.getValue();
                                    if (s1.contains(s2)) {
                                        skip = true;
                                        break;
                                    } else if (s2.contains(s1)) {
                                        conditionMemory.fieldConditions.remove(like);
                                    }
                                }
                                if (skip) {
                                    continue;
                                }
                                orConditionMemory.likeSet.add(condition);
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                            case NOT: {
                                if (orConditionMemory.not != null) {
                                    conditionMemory.fieldConditions.remove(orConditionMemory.not);
                                    continue;
                                }
                                orConditionMemory.not = condition;
                                conditionMemory.fieldConditions.add(condition);
                                break;
                            }
                        }
                        break;
                    }
                }
            }
            simplify(conditionMemory);
            fieldsConditions.add(makeCondition(complexType, conditionMemory.fieldConditions));
        }
        return makeCondition(complexType, fieldsConditions);
    }

    private Collection<SimpleCondition> sort(Collection<SimpleCondition> conditions) {
        final Function<ICondition.SimpleType, Integer> priorityFunction = type -> {
            switch (type) {
                case EQ:
                    return 0;
                case LT:
                    return 1;
                case LTE:
                    return 2;
                case GT:
                    return 3;
                case GTE:
                    return 4;
                case LIKE:
                    return 5;
                case NOT:
                    return 6;
                default:
                    throw new RuntimeException("unknown type " + type);
            }
        };
        return conditions.stream().sorted(Comparator
                .comparing(SimpleCondition::getType, (c1, c2) -> priorityFunction.apply(c2).compareTo(priorityFunction.apply(c1)))
                .thenComparing(SimpleCondition::getValue, (v1, v2) -> v2.compareTo(v1))).collect(Collectors.toList());
    }

    private void simplify(ConditionMemory conditionMemory) throws ConditionException {
        if (conditionMemory instanceof AndConditionMemory) {
            final AndConditionMemory andConditionMemory = (AndConditionMemory) conditionMemory;
            if (andConditionMemory.lte != null && andConditionMemory.gte != null && andConditionMemory.lte.getValue().compareTo(andConditionMemory.gte.getValue()) == 0) {
                conditionMemory.fieldConditions.remove(andConditionMemory.lte);
                conditionMemory.fieldConditions.remove(andConditionMemory.gte);
                conditionMemory.fieldConditions.add(SimpleCondition.make(EQ, andConditionMemory.lte.getField(), andConditionMemory.lte.getValue()));
            }
            for (final SimpleCondition not : andConditionMemory.notSet) {
                if (andConditionMemory.lte != null && andConditionMemory.lte.getValue().compareTo(not.getValue()) == 0) {
                    conditionMemory.fieldConditions.remove(not);
                    conditionMemory.fieldConditions.remove(andConditionMemory.lte);
                    conditionMemory.fieldConditions.add(SimpleCondition.make(LT, not.getField(), not.getValue()));
                }
                if (andConditionMemory.gte != null && andConditionMemory.gte.getValue().compareTo(not.getValue()) == 0) {
                    conditionMemory.fieldConditions.remove(not);
                    conditionMemory.fieldConditions.remove(andConditionMemory.gte);
                    conditionMemory.fieldConditions.add(SimpleCondition.make(GT, not.getField(), not.getValue()));
                }
            }
        } else if (conditionMemory instanceof OrConditionMemory) {
            final OrConditionMemory orConditionMemory = (OrConditionMemory) conditionMemory;
            if (orConditionMemory.lt != null && orConditionMemory.gt != null && orConditionMemory.lt.getValue().compareTo(orConditionMemory.gt.getValue()) == 0) {
                conditionMemory.fieldConditions.remove(orConditionMemory.lt);
                conditionMemory.fieldConditions.remove(orConditionMemory.gt);
                conditionMemory.fieldConditions.add(SimpleCondition.make(NOT, orConditionMemory.lt.getField(), orConditionMemory.lt.getValue()));
            }
            for (final SimpleCondition eq : orConditionMemory.eqSet) {
                if (orConditionMemory.lt != null && orConditionMemory.lt.getValue().compareTo(eq.getValue()) == 0) {
                    conditionMemory.fieldConditions.remove(eq);
                    conditionMemory.fieldConditions.remove(orConditionMemory.lt);
                    conditionMemory.fieldConditions.add(SimpleCondition.make(LTE, eq.getField(), eq.getValue()));
                }
                if (orConditionMemory.gt != null && orConditionMemory.gt.getValue().compareTo(eq.getValue()) == 0) {
                    conditionMemory.fieldConditions.remove(eq);
                    conditionMemory.fieldConditions.remove(orConditionMemory.gt);
                    conditionMemory.fieldConditions.add(SimpleCondition.make(GTE, eq.getField(), eq.getValue()));
                }
            }
        }
    }

    private <T extends ICondition> ICondition makeCondition(ICondition.ComplexType complexType, Collection<T> conditions) throws ConditionException {
        if (conditions.size() == 0) {
            return ICondition.empty;
        }
        final Set<ICondition> conditionSet = new HashSet<>();
        String field = null;
        boolean hasSameField = true;
        for (ICondition condition : conditions) {
            final ICondition addedCondition;
            if (condition instanceof ComplexCondition) {
                addedCondition = makeCondition(complexType, ((ComplexCondition<T>) condition).getConditions());
            } else {
                addedCondition = condition;
            }
            if (addedCondition instanceof FieldCondition) {
                final FieldCondition fieldCondition = (FieldCondition) addedCondition;
                if (field == null) {
                    field = fieldCondition.getField();
                } else if (!fieldCondition.getField().equals(field)) {
                    hasSameField = false;
                }
            }
            conditionSet.add(addedCondition);
        }
        if (conditionSet.size() == 1) {
            return conditionSet.iterator().next();
        }
        if (hasSameField) {
            return FieldComplexCondition.make(complexType, conditionSet);
        }
        return MultiComplexCondition.make(complexType, conditionSet);
    }

    private boolean check(SimpleCondition conditionFirst, SimpleCondition conditionSecond, Function<Integer, Boolean> function, String error) throws ConditionException {
        if (conditionSecond == null || function.apply(conditionFirst.getValue().compareTo(conditionSecond.getValue()))) {
            return true;
        }
        if (error != null) {
            throw makeConditionException(error, conditionFirst, conditionSecond);
        }
        return false;
    }

    private ConditionException makeConditionException(String error, SimpleCondition conditionFirst, SimpleCondition conditionSecond) {
        return new ConditionException(error + ", conditionFirst " + conditionFirst + ", conditionSecond " + conditionSecond);
    }

    private boolean checkAndRemove(SimpleCondition conditionFirst, SimpleCondition conditionSecond, Collection<SimpleCondition> fieldConditions, Function<Integer, Boolean> function, String error) throws ConditionException {
        if (conditionFirst == null || conditionSecond == null) {
            return false;
        }
        if (function.apply(conditionFirst.getValue().compareTo(conditionSecond.getValue()))) {
            fieldConditions.remove(conditionSecond);
            return true;
        } else if (error != null) {
            throw makeConditionException(error, conditionFirst, conditionSecond);
        }
        return false;
    }

    private void substitute(SimpleCondition conditionFirst, SimpleCondition conditionSecond, Collection<SimpleCondition> fieldConditions, Function<Integer, Boolean> function, Consumer<SimpleCondition> consumer) {
        if (conditionSecond == null || function.apply(conditionFirst.getValue().compareTo(conditionSecond.getValue()))) {
            if (conditionSecond != null) {
                fieldConditions.remove(conditionSecond);
            }
            consumer.accept(conditionFirst);
            fieldConditions.add(conditionFirst);
        }
    }

    private boolean checkAndFillMap(ComplexCondition<ICondition> complexCondition, ICondition.ComplexType complexType, Map<String, Collection<SimpleCondition>> conditionMap) {
        if (!complexType.equals(complexCondition.getType())) {
            return false;
        }
        for (ICondition condition : complexCondition.getConditions()) {
            if (condition instanceof ComplexCondition) {
                if (!checkAndFillMap((ComplexCondition) condition, complexType, conditionMap)) {
                    return false;
                }
            } else if (condition instanceof SimpleCondition) {
                final SimpleCondition simpleCondition = (SimpleCondition) condition;
                final Collection<SimpleCondition> simpleConditions = conditionMap.computeIfAbsent(simpleCondition.getField(), k -> new HashSet<>());
                simpleConditions.add(simpleCondition);
            }
        }
        return true;
    }

    private ComplexCondition<ICondition> parseComplexCondition(String input) throws ConditionException {
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
        return MultiComplexCondition.make(type, conditions);
    }

    private SimpleCondition parseSimpleCondition(String input) throws ConditionException {
        final String formatted = input.trim();
        final ICondition.SimpleType conditionType = getConditionSign(formatted, ICondition.SimpleType.values());
        final String[] parts = formatted.split(conditionType.toString());
        if (parts.length != 2) {
            throw new ConditionException("wrong condition patter : " + input);
        }
        final Pair<String, Comparable> pair = parseValue(parts);
        if (ICondition.SimpleType.LIKE == conditionType && !(pair.getSecond() instanceof String)) {
            throw new ConditionException("wrong type from LIKE condition " + pair.getSecond().getClass() + " for field " + pair.getFirst());
        }
        return SimpleCondition.make(conditionType, pair.getFirst(), pair.getSecond());
    }

    private Pair<String, Comparable> parseValue(String[] parts) throws ConditionException {
        final String field = parts[0].trim();
        final String valueStr = parts[1].trim();
        checkFieldName(field);
        final Comparable value;
        if (!"null".equals(valueStr)) {
            try {
                value = modelService.getValue(field, valueStr);
            } catch (NumberFormatException e) {
                throw new ConditionException("wrong value type for field : " + field + ", value : " + valueStr);
            }
        } else {
            value = null;
        }
        return new Pair<>(field, value);
    }

    private void checkFieldName(String field) throws ConditionException {
        if (!modelService.contains(field)) {
            throw new ConditionException("unknown field : " + field);
        }
    }

    private <T> T getConditionSign(String formatted, T[] values) throws ConditionException {
        if (values instanceof ICondition.SimpleType[]) {
            if (formatted.contains(ICondition.SimpleType.GTE.toString())) {
                return (T) ICondition.SimpleType.GTE;
            } else if (formatted.contains(ICondition.SimpleType.LTE.toString())) {
                return (T) ICondition.SimpleType.LTE;
            }
        }
        for (T type : values) {
            final String typeStr = type.toString();
            if (formatted.contains(typeStr)) {
                return type;
            }
        }
        throw new ConditionException("input String does not contains allowed condition");
    }

    @Override
    public <T> boolean check(T value, ICondition condition) {
        if (condition instanceof SimpleCondition) {
            return check(value, (SimpleCondition) condition);
        } else if (condition instanceof ComplexCondition) {
            return check(value, (ComplexCondition<ICondition>) condition);
        } else if (condition instanceof EmptyCondition) {
            return true;
        }
        throw new IllegalArgumentException("Unknown condition class : " + condition.getClass());
    }

    private <T> boolean check(T value, ComplexCondition<ICondition> condition) {
        Boolean result = null;
        boolean isFirst = true;
        for (ICondition innerCondition : condition.getConditions()) {
            if (Utils.checkStopConditions(isFirst, condition.getType(), result, r -> !r)) {
                break;
            }
            if (isFirst) {
                result = check(value, innerCondition);
                isFirst = false;
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
                    throw new IllegalArgumentException("Unknown complex type : " + condition.getType());
            }
        }
        if (result == null) {
            return true;
        }
        return result;
    }

    private <T> boolean check(T input, SimpleCondition condition) {
        if (condition.getValue() == null && !(EQ.equals(condition.getType()) || NOT.equals(condition.getType()))) {
            throw new IllegalArgumentException("wrong condition " + condition.getField() + ", null values allowed only for EQ and NOT");
        }
        final Comparable value;
        if (input instanceof Row) {
            final Row row = (Row) input;
            value = row.getFields().get(condition.getField());
        } else if (input instanceof Comparable) {
            value = (Comparable) input;
        } else {
            value = null;
        }
        if (value == null) {
            if (condition.getValue() == null) {
                return EQ.equals(condition.getType());
            }
            return NOT.equals(condition.getType());
        }
        if (condition.getValue() == null) {
            return NOT.equals(condition.getType());
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
                throw new IllegalArgumentException("Unknown simple type : " + condition.getType());
        }
    }

    private static abstract class ConditionMemory {
        public final Set<SimpleCondition> fieldConditions = new HashSet<>();
        public SimpleCondition lt;
        public SimpleCondition lte;
        public SimpleCondition gt;
        public SimpleCondition gte;

        public void setLt(SimpleCondition lt) {
            this.lt = lt;
        }

        public void setLte(SimpleCondition lte) {
            this.lte = lte;
        }

        public void setGt(SimpleCondition gt) {
            this.gt = gt;
        }

        public void setGte(SimpleCondition gte) {
            this.gte = gte;
        }
    }

    private static class AndConditionMemory extends ConditionMemory {
        public final Set<SimpleCondition> notSet = new HashSet<>();
        public SimpleCondition eq;
        public SimpleCondition like;

        public void setEq(SimpleCondition eq) {
            this.eq = eq;
        }

        public void setLike(SimpleCondition like) {
            this.like = like;
        }
    }

    private static class OrConditionMemory extends ConditionMemory {
        public final Set<SimpleCondition> eqSet = new HashSet<>();
        public final Set<SimpleCondition> likeSet = new HashSet<>();
        public SimpleCondition not;

        public void setNot(SimpleCondition not) {
            this.not = not;
        }
    }
}