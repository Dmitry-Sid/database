package sample.model;

import sample.model.pojo.Application;

public interface Repository {

    public void add(Application application);

    public void delete(String id);

    public Application get(String id);
}
