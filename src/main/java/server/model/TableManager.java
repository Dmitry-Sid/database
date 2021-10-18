package server.model;

public interface TableManager {
    void create(String tableName);

    void delete(String tableName);

    ServiceHolder getServiceHolder(String tableName);

    public static class ServiceHolder {
        public final RowRepository rowRepository;
        public final ModelService modelService;

        public ServiceHolder(RowRepository rowRepository, ModelService modelService) {
            this.rowRepository = rowRepository;
            this.modelService = modelService;
        }
    }
}
