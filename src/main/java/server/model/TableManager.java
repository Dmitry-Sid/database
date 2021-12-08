package server.model;

import java.util.Set;

public interface TableManager extends Destroyable {
    void create(String tableName);

    void delete(String tableName);

    ServiceHolder getServiceHolder(String tableName);

    Set<String> getTables();

    class ServiceHolder {
        public final RowRepository rowRepository;
        public final ModelService modelService;
        public final ConditionService conditionService;

        public ServiceHolder(RowRepository rowRepository, ModelService modelService, ConditionService conditionService) {
            this.rowRepository = rowRepository;
            this.modelService = modelService;
            this.conditionService = conditionService;
        }
    }
}
