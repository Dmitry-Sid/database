package server.model;

import java.io.File;
import java.util.Collection;
import java.util.List;
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

    private static boolean isWindows() {
        return OSType.WIN.equals(OS_TYPE);
    }

    private enum OSType {
        WIN, MAC, LINUX, OTHER;
    }
}
