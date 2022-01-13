package server.model.impl;

import org.apache.commons.lang3.StringUtils;
import server.model.ConditionException;
import server.model.ConditionService;
import server.model.ModelService;
import server.model.pojo.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static server.model.pojo.ICondition.SimpleType.EQ;
import static server.model.pojo.ICondition.SimpleType.NOT;

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

    private ICondition transform(ComplexCondition complexCondition) throws ConditionException {
        final ICondition.ComplexType complexType = complexCondition.getType();
        final Map<String, Collection<SimpleCondition>> conditionMap = new LinkedHashMap<>();
        if (!checkAndFillMap(complexCondition, complexType, conditionMap)) {
            return complexCondition;
        }
        final Set<ICondition> fieldsConditions = new HashSet<>();
        for (Collection<SimpleCondition> conditions : conditionMap.values()) {
            final Set<SimpleCondition> fieldConditions = new HashSet<>();
            final AtomicReference<SimpleCondition> lt = new AtomicReference<>();
            final AtomicReference<SimpleCondition> lte = new AtomicReference<>();
            final AtomicReference<SimpleCondition> gt = new AtomicReference<>();
            final AtomicReference<SimpleCondition> gte = new AtomicReference<>();
            switch (complexType) {
                case AND: {
                    final AtomicReference<SimpleCondition> eq = new AtomicReference<>();
                    final AtomicReference<SimpleCondition> like = new AtomicReference<>();
                    final Set<SimpleCondition> notSet = new HashSet<>();
                    for (SimpleCondition condition : conditions) {
                        switch (condition.getType()) {
                            case EQ: {
                                checkAndRemove(condition, lt, fieldConditions, v -> v < 0, "EQ condition cannot have greater or equal value than LT value");
                                checkAndRemove(condition, lte, fieldConditions, v -> v <= 0, "EQ condition cannot have greater value than LTE value");
                                checkAndRemove(condition, gt, fieldConditions, v -> v > 0, "EQ condition cannot have lower or equal value than GT value");
                                checkAndRemove(condition, gte, fieldConditions, v -> v >= 0, "EQ condition cannot have lower value than GTE value");
                                for (Iterator<SimpleCondition> iterator = notSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v == 0, "EQ condition cannot have same value as NOT value")) {
                                        iterator.remove();
                                    }
                                }
                                if (like.get() != null) {
                                    final String s = (String) condition.getValue();
                                    if (s.contains((String) like.get().getValue())) {
                                        remove(fieldConditions, like);
                                    } else {
                                        throw makeConditionException("EQ condition cannot have other value than LIKE value", condition, like.get());
                                    }
                                }
                                if (eq.get() == null || condition.getValue().compareTo(eq.get().getValue()) == 0) {
                                    eq.set(condition);
                                } else {
                                    throw makeConditionException("cannot be more than one EQ condition inside AND condition", condition, eq.get());
                                }
                                fieldConditions.add(condition);
                                break;
                            }
                            case LT: {
                                final Runnable addRunnable = () -> {
                                    if (lt.get() == null || condition.getValue().compareTo(lt.get().getValue()) < 0) {
                                        if (lt.get() != null) {
                                            remove(fieldConditions, lt);
                                        }
                                        lt.set(condition);
                                        fieldConditions.add(condition);
                                    }
                                };
                                if (eq.get() != null) {
                                    if (condition.getValue().compareTo(eq.get().getValue()) <= 0) {
                                        throw makeConditionException("LT condition cannot have lower or equal value than EQ value", condition, eq.get());
                                    } else {
                                        continue;
                                    }
                                }
                                if (lte.get() != null) {
                                    if (condition.getValue().compareTo(lte.get().getValue()) <= 0) {
                                        remove(fieldConditions, lte);
                                        addRunnable.run();
                                    }
                                    continue;
                                }
                                check(condition, gt, v -> v <= 0, "LT condition cannot have lower or equal value than GT value");
                                check(condition, gte, v -> v <= 0, "LT condition cannot have lower or equal value than GTE value");
                                for (Iterator<SimpleCondition> iterator = notSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v <= 0, null)) {
                                        iterator.remove();
                                    }
                                }
                                addRunnable.run();
                                break;
                            }
                            case LTE: {
                                final Runnable addRunnable = () -> {
                                    if (lte.get() == null || condition.getValue().compareTo(lte.get().getValue()) < 0) {
                                        if (lte.get() != null) {
                                            remove(fieldConditions, lte);
                                        }
                                        lte.set(condition);
                                        fieldConditions.add(condition);
                                    }
                                };
                                if (eq.get() != null) {
                                    final int compareResult = condition.getValue().compareTo(eq.get().getValue());
                                    if (compareResult < 0) {
                                        throw makeConditionException("LTE condition cannot have lower value than EQ value", condition, eq.get());
                                    } else {
                                        continue;
                                    }
                                }
                                if (lt.get() != null) {
                                    if (condition.getValue().compareTo(lt.get().getValue()) < 0) {
                                        remove(fieldConditions, lt);
                                        addRunnable.run();
                                    }
                                    continue;
                                }
                                check(condition, gt, v -> v <= 0, "LTE condition cannot have lower or equal value than GT value");
                                check(condition, gte, v -> v < 0, "LT condition cannot have lower value than GTE value");
                                for (Iterator<SimpleCondition> iterator = notSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v < 0, null)) {
                                        iterator.remove();
                                    }
                                }
                                addRunnable.run();
                                break;
                            }
                            case GT: {
                                final Runnable addRunnable = () -> {
                                    if (gt.get() == null || condition.getValue().compareTo(gt.get().getValue()) > 0) {
                                        if (gt.get() != null) {
                                            remove(fieldConditions, gt);
                                        }
                                        gt.set(condition);
                                        fieldConditions.add(condition);
                                    }
                                };
                                if (eq.get() != null) {
                                    if (condition.getValue().compareTo(eq.get().getValue()) >= 0) {
                                        throw makeConditionException("GT condition cannot have greater or equal value than EQ value", condition, eq.get());
                                    } else {
                                        continue;
                                    }
                                }
                                check(condition, lt, v -> v >= 0, "GT condition cannot have greater or equal value than LT value");
                                check(condition, lte, v -> v >= 0, "GT condition cannot have greater or equal value than LTE value");
                                if (gte.get() != null) {
                                    if (condition.getValue().compareTo(gte.get().getValue()) >= 0) {
                                        remove(fieldConditions, gte);
                                        addRunnable.run();
                                    }
                                    continue;
                                }
                                for (Iterator<SimpleCondition> iterator = notSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v >= 0, null)) {
                                        iterator.remove();
                                    }
                                }
                                addRunnable.run();
                                break;
                            }
                            case GTE: {
                                final Runnable addRunnable = () -> {
                                    if (gte.get() == null || condition.getValue().compareTo(gte.get().getValue()) > 0) {
                                        if (gte.get() != null) {
                                            remove(fieldConditions, gte);
                                        }
                                        gte.set(condition);
                                        fieldConditions.add(condition);
                                    }
                                };
                                if (eq.get() != null) {
                                    final int compareResult = condition.getValue().compareTo(eq.get().getValue());
                                    if (compareResult > 0) {
                                        throw makeConditionException("GTE condition cannot have greater value than EQ value", condition, eq.get());
                                    } else {
                                        continue;
                                    }
                                }
                                check(condition, lt, v -> v >= 0, "GTE condition cannot have greater or equal value than LT value");
                                check(condition, lte, v -> v > 0, "GTE condition cannot have greater value than LTE value");
                                if (gt.get() != null) {
                                    if (condition.getValue().compareTo(gt.get().getValue()) > 0) {
                                        remove(fieldConditions, gt);
                                        addRunnable.run();
                                    }
                                    continue;
                                }
                                for (Iterator<SimpleCondition> iterator = notSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v > 0, null)) {
                                        iterator.remove();
                                    }
                                }
                                addRunnable.run();
                                break;
                            }
                            case LIKE: {
                                if (eq.get() != null) {
                                    final String s = (String) eq.get().getValue();
                                    if (s.contains((String) condition.getValue())) {
                                        continue;
                                    } else {
                                        throw makeConditionException("LIKE condition cannot have other value than EQ value", condition, eq.get());
                                    }
                                }
                                for (SimpleCondition not : notSet) {
                                    final String s = (String) not.getValue();
                                    if (s.contains((String) condition.getValue())) {
                                        throw makeConditionException("LIKE condition cannot have same value as NOT value", condition, eq.get());
                                    }
                                }
                                like.set(condition);
                                fieldConditions.add(condition);
                                break;
                            }
                            case NOT: {
                                check(condition, eq, v -> v == 0, "NOT condition cannot have same value as EQ value");
                                if (checkAndAdd(condition, lt, fieldConditions, v -> v <= 0)) {
                                    notSet.add(condition);
                                    continue;
                                }
                                if (checkAndAdd(condition, lte, fieldConditions, v -> v < 0)) {
                                    notSet.add(condition);
                                    continue;
                                }
                                if (checkAndAdd(condition, gt, fieldConditions, v -> v >= 0)) {
                                    notSet.add(condition);
                                    continue;
                                }
                                if (checkAndAdd(condition, gte, fieldConditions, v -> v > 0)) {
                                    notSet.add(condition);
                                    continue;
                                }
                                notSet.add(condition);
                                fieldConditions.add(condition);
                                break;
                            }
                        }
                    }
                    break;
                }
                case OR: {
                    final Set<SimpleCondition> eqSet = new HashSet<>();
                    final Set<SimpleCondition> likeSet = new HashSet<>();
                    final AtomicReference<SimpleCondition> not = new AtomicReference<>();
                    for (SimpleCondition condition : conditions) {
                        switch (condition.getType()) {
                            case EQ: {
                                if (checkAndAdd(condition, lt, fieldConditions, v -> v >= 0)) {
                                    eqSet.add(condition);
                                    continue;
                                }
                                if (checkAndAdd(condition, lte, fieldConditions, v -> v > 0)) {
                                    eqSet.add(condition);
                                    continue;
                                }
                                if (checkAndAdd(condition, gt, fieldConditions, v -> v <= 0)) {
                                    eqSet.add(condition);
                                    continue;
                                }
                                if (checkAndAdd(condition, gte, fieldConditions, v -> v < 0)) {
                                    eqSet.add(condition);
                                    continue;
                                }
                                if (checkAndRemove(condition, not, fieldConditions, v -> v == 0, null)) {
                                    continue;
                                }
                                boolean contains = false;
                                for (SimpleCondition like : likeSet) {
                                    final String s = (String) like.getValue();
                                    if (s.contains((String) condition.getValue())) {
                                        contains = true;
                                        break;
                                    }
                                }
                                if (contains) {
                                    continue;
                                }
                                eqSet.add(condition);
                                fieldConditions.add(condition);
                                break;
                            }
                            case LT: {
                                for (Iterator<SimpleCondition> iterator = eqSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v > 0, null)) {
                                        iterator.remove();
                                    }
                                }
                                checkAndRemove(condition, lte, fieldConditions, v -> v >= 0, null);
                                if (checkAndRemove(condition, gt, fieldConditions, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, gte, fieldConditions, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, not, fieldConditions, v -> v > 0, null)) {
                                    continue;
                                }
                                if (lt.get() == null || condition.getValue().compareTo(lt.get().getValue()) > 0) {
                                    if (lt.get() != null) {
                                        remove(fieldConditions, lt);
                                    }
                                    lt.set(condition);
                                    fieldConditions.add(condition);
                                }
                                break;
                            }
                            case LTE: {
                                for (Iterator<SimpleCondition> iterator = eqSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v >= 0, null)) {
                                        iterator.remove();
                                    }
                                }
                                checkAndRemove(condition, lt, fieldConditions, v -> v >= 0, null);
                                if (checkAndRemove(condition, gt, fieldConditions, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, gte, fieldConditions, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, not, fieldConditions, v -> v >= 0, null)) {
                                    continue;
                                }
                                if (lte.get() == null || condition.getValue().compareTo(lte.get().getValue()) > 0) {
                                    if (lte.get() != null) {
                                        remove(fieldConditions, lte);
                                    }
                                    lte.set(condition);
                                    fieldConditions.add(condition);
                                }
                                break;
                            }
                            case GT: {
                                for (Iterator<SimpleCondition> iterator = eqSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v < 0, null)) {
                                        iterator.remove();
                                    }
                                }
                                checkAndRemove(condition, gte, fieldConditions, v -> v <= 0, null);
                                if (checkAndRemove(condition, lt, fieldConditions, v -> v <= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, lte, fieldConditions, v -> v <= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, not, fieldConditions, v -> v < 0, null)) {
                                    continue;
                                }
                                if (gt.get() == null || condition.getValue().compareTo(gt.get().getValue()) > 0) {
                                    if (gt.get() != null) {
                                        remove(fieldConditions, gt);
                                    }
                                    gt.set(condition);
                                    fieldConditions.add(condition);
                                }
                                break;
                            }
                            case GTE: {
                                for (Iterator<SimpleCondition> iterator = eqSet.iterator(); iterator.hasNext(); ) {
                                    if (checkAndRemove(condition, new AtomicReference<>(iterator.next()), fieldConditions, v -> v <= 0, null)) {
                                        iterator.remove();
                                    }
                                }
                                checkAndRemove(condition, gt, fieldConditions, v -> v <= 0, null);
                                if (checkAndRemove(condition, lt, fieldConditions, v -> v <= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, lte, fieldConditions, v -> v <= 0, null)) {
                                    continue;
                                }
                                if (checkAndRemove(condition, not, fieldConditions, v -> v <= 0, null)) {
                                    continue;
                                }
                                if (gte.get() == null || condition.getValue().compareTo(gte.get().getValue()) < 0) {
                                    if (gte.get() != null) {
                                        remove(fieldConditions, gte);
                                    }
                                    gte.set(condition);
                                    fieldConditions.add(condition);
                                }
                                break;
                            }
                            case LIKE: {
                                for (Iterator<SimpleCondition> iterator = Stream.concat(eqSet.stream(), likeSet.stream()).iterator(); iterator.hasNext(); ) {
                                    final SimpleCondition eq = iterator.next();
                                    final String s1 = (String) condition.getValue();
                                    final String s2 = (String) eq.getValue();
                                    if (s1.contains(s2)) {
                                        remove(fieldConditions, new AtomicReference<>(eq));
                                        iterator.remove();
                                    }
                                }
                                likeSet.add(condition);
                                fieldConditions.add(condition);
                                break;
                            }
                            case NOT: {
                                if (not.get() != null) {
                                    remove(fieldConditions, not);
                                    continue;
                                }
                                checkAndRemove(condition, lt, fieldConditions, v -> v <= 0, null);
                                if (lte.get() != null) {
                                    if (condition.getValue().compareTo(lte.get().getValue()) > 0) {
                                        fieldConditions.remove(lte.get());
                                    } else {
                                        continue;
                                    }
                                }
                                checkAndRemove(condition, gt, fieldConditions, v -> v >= 0, null);
                                if (gte.get() != null) {
                                    if (condition.getValue().compareTo(gte.get().getValue()) < 0) {
                                        fieldConditions.remove(gte.get());
                                    } else {
                                        continue;
                                    }
                                }
                                not.set(condition);
                                fieldConditions.add(condition);
                                break;
                            }
                        }
                    }
                    break;
                }
            }
            fieldsConditions.add(makeCondition(complexType, fieldConditions));
        }
        return makeCondition(complexType, fieldsConditions);
    }

    private <T extends ICondition> ICondition makeCondition(ICondition.ComplexType complexType, Collection<T> conditions) throws ConditionException {
        if (conditions.size() == 0) {
            throw new ConditionException("not conditions at all");
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


    private void remove(Collection<SimpleCondition> fieldList, AtomicReference<SimpleCondition> atomicReference) {
        fieldList.remove(atomicReference.get());
        atomicReference.set(null);
    }

    private void check(SimpleCondition conditionFirst, AtomicReference<SimpleCondition> conditionSecond, Function<Integer, Boolean> function, String error) throws ConditionException {
        if (conditionSecond.get() != null && function.apply(conditionFirst.getValue().compareTo(conditionSecond.get().getValue()))) {
            throw makeConditionException(error, conditionFirst, conditionSecond.get());
        }
    }

    private ConditionException makeConditionException(String error, SimpleCondition conditionFirst, SimpleCondition conditionSecond) {
        return new ConditionException(error + ", conditionFirst " + conditionFirst + ", conditionSecond " + conditionSecond);
    }

    private boolean checkAndRemove(SimpleCondition conditionFirst, AtomicReference<SimpleCondition> conditionSecond, Collection<SimpleCondition> fieldConditions, Function<Integer, Boolean> function, String error) throws ConditionException {
        if (conditionFirst == null || conditionSecond.get() == null) {
            return false;
        }
        if (function.apply(conditionFirst.getValue().compareTo(conditionSecond.get().getValue()))) {
            remove(fieldConditions, conditionSecond);
            return true;
        } else if (error != null) {
            throw makeConditionException(error, conditionFirst, conditionSecond.get());
        }
        return false;
    }

    private boolean checkAndAdd(SimpleCondition condition, AtomicReference<SimpleCondition> compared, Collection<SimpleCondition> fieldConditions, Function<Integer, Boolean> function) {
        if (compared.get() == null || function.apply(condition.getValue().compareTo(compared.get().getValue()))) {
            fieldConditions.add(condition);
            return true;
        }
        return false;
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

    private ComplexCondition parseComplexCondition(String input) throws ConditionException {
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
}