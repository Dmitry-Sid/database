package server.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public interface ModelService {
    public static final List<Class<?>> types = Arrays.asList(Byte.class, Character.class, Short.class, Integer.class,
            Long.class, Float.class, Double.class, String.class);

    public boolean contains(String field);

    public Comparable getValue(String field, String value);

    public void add(String field, Class<?> type);

    public void delete(String field);

    public void addIndex(String field);

    public void deleteIndex(String field);

    public List<FieldInfo> getFields();

    public Set<String> getIndexedFields();

    public void subscribeOnFieldsChanges(Consumer<Set<String>> fieldsConsumer);

    public void subscribeOnIndexesChanges(Consumer<Set<String>> fieldsConsumer);

    public static class FieldInfo implements Serializable {
        private static final long serialVersionUID = 3990344518009191295L;
        private String name;
        private Class<?> type;
        private boolean isIndex;

        public FieldInfo() {

        }

        public FieldInfo(String name, Class<?> type, boolean isIndex) {
            this.name = name;
            this.type = type;
            this.isIndex = isIndex;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setType(Class<?> type) {
            this.type = type;
        }

        public Class<?> getType() {
            return type;
        }

        public void setIndex(boolean index) {
            isIndex = index;
        }

        public boolean isIndex() {
            return isIndex;
        }
    }
}
