package server.model;

import java.util.Set;

public interface TableManager extends Destroyable {
    void create(String tableName);

    void delete(String tableName);

    ServiceHolder getServiceHolder(String tableName);

    Set<String> getTables();

    public static class ServiceHolder {
        public final RowRepository rowRepository;
        public final ModelService modelService;

        public ServiceHolder(RowRepository rowRepository, ModelService modelService) {
            this.rowRepository = rowRepository;
            this.modelService = modelService;
        }
    }
}
