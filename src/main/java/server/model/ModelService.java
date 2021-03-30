package server.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public interface ModelService {
    List<Class<?>> types = Arrays.asList(Byte.class, Character.class, Short.class, Integer.class,
            Long.class, Float.class, Double.class, String.class);

    boolean contains(String field);

    Comparable getValue(String field, String value);

    void add(String field, Class<?> type);

    void delete(String field);

    void addIndex(String field);

    void deleteIndex(String field);

    List<FieldInfo> getFields();

    Set<String> getIndexedFields();

    void subscribeOnFieldsChanges(Consumer<Set<String>> fieldsConsumer);

    void subscribeOnIndexesChanges(Consumer<Set<String>> fieldsConsumer);

    class FieldInfo implements Serializable {
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

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Class<?> getType() {
            return type;
        }

        public void setType(Class<?> type) {
            this.type = type;
        }

        public boolean isIndex() {
            return isIndex;
        }

        public void setIndex(boolean index) {
            isIndex = index;
        }
    }
}
