package sample.model;

import java.util.Set;

public interface ModelService {

    public Set<String> getIndexedFields();

    public boolean containsField(String field);

    public Comparable getValue(String field, String value);

}
