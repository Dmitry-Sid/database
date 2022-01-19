package server.model;

import server.model.pojo.ComplexCondition;
import server.model.pojo.ICondition;
import server.model.pojo.SimpleCondition;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Utils {
    private static OSType OS_TYPE = determineOSType();

    private static OSType determineOSType() {
        final String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("win")) {
            return OSType.WIN;
        } else if (osName.contains("mac")) {
            return OSType.MAC;
        } else if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix")) {
            return OSType.LINUX;
        }
        return OSType.OTHER;
    }

    public static <V> boolean isFull(Collection<V> collection, int size) {
        return size >= 0 && collection.size() >= size;
    }

    public static <V> boolean fillToFull(Collection<V> mainCollection, int size, Collection<V> collection) {
        if (isFull(mainCollection, size)) {
            final List<V> reducedList = mainCollection.stream().limit(size).collect(Collectors.toList());
            mainCollection.clear();
            mainCollection.addAll(reducedList);
            return true;
        }
        for (V value : collection) {
            if (isFull(mainCollection, size)) {
                return true;
            }
            mainCollection.add(value);
        }
        return false;
    }

    public static void createDirectoryTree(File file) {
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public static String getFullPath(String basePath, String... subtrees) {
        final String delimiter = "/";
        final StringBuilder sb = new StringBuilder(basePath);
        for (String subtree : subtrees) {
            sb.append(subtree).append(delimiter);
        }
        return sb.toString();
    }

    public static <T> void compareAndRun(T actual, T previous, Runnable action) {
        if (!actual.equals(previous)) {
            action.run();
        }
    }

    public static <T> Set<T> collectConditions(ICondition iCondition, Function<SimpleCondition, Collection<T>> function) {
        if (iCondition instanceof SimpleCondition) {
            return new HashSet<>(function.apply((SimpleCondition) iCondition));
        }
        final ComplexCondition<ICondition> complexCondition = (ComplexCondition<ICondition>) iCondition;
        final Set<T> set = new HashSet<>();
        boolean isFirst = true;
        for (ICondition condition : complexCondition.getConditions()) {
            if (!isFirst && ICondition.ComplexType.AND == complexCondition.getType() && set.isEmpty()) {
                break;
            }
            final Collection<T> result;
            if (condition instanceof ComplexCondition) {
                result = collectConditions(condition, function);
            } else {
                result = function.apply((SimpleCondition) condition);
            }
            if (isFirst) {
                set.addAll(result);
                isFirst = false;
                continue;
            }
            switch (complexCondition.getType()) {
                case OR:
                    set.addAll(result);
                    break;
                case AND:
                    set.retainAll(result);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown complex type : " + complexCondition.getType());
            }
        }
        return set;
    }

    public static <T> boolean checkStopConditions(boolean isFirst, ICondition.ComplexType complexType, T value, Function<T, Boolean> function) {
        if (value == null) {
            return false;
        }
        if (!isFirst && ICondition.ComplexType.AND == complexType && function.apply(value)) {
            return true;
        }
        return !isFirst && ICondition.ComplexType.OR == complexType && !function.apply(value);
    }

    public static <T extends Comparable<T>> boolean skipTreeSearch(SimpleCondition condition, T parentKey, T key) {
        if (ICondition.SimpleType.GT == condition.getType() || ICondition.SimpleType.GTE == condition.getType()) {
            return parentKey.compareTo(key) > 0 && condition.getValue().compareTo(parentKey) >= 0;
        } else if (ICondition.SimpleType.LT == condition.getType() || ICondition.SimpleType.LTE == condition.getType()) {
            return parentKey.compareTo(key) < 0 && condition.getValue().compareTo(parentKey) <= 0;
        }
        return false;
    }


    private static boolean isWindows() {
        return OSType.WIN.equals(OS_TYPE);
    }

    private enum OSType {
        WIN, MAC, LINUX, OTHER
    }
}
