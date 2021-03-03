package sample.model;

import java.util.Set;

public class ModelServiceImpl implements ModelService {
    @Override
    public Set<String> getIndexedFields() {
        return null;
    }

    @Override
    public boolean containsField(String field) {
        return "String".equalsIgnoreCase(field) || "int".equalsIgnoreCase(field) || "double".equalsIgnoreCase(field);
    }

    @Override
    public Comparable getValue(String field, String value) {
        if ("String".equalsIgnoreCase(field)) {
            return value;
        } else if ("int".equalsIgnoreCase(field)) {
            return Integer.parseInt(value);
        } else if ("double".equalsIgnoreCase(field)) {
            return Double.parseDouble(value);
        }
        throw new RuntimeException("unknown field " + field);
    }
}
