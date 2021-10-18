package server.model.impl;

import server.model.ModelService;
import server.model.RowRepository;
import server.model.TableManager;
import server.model.TableServiceFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableManagerImpl implements TableManager {
    private final Map<String, ServiceHolder> map = new ConcurrentHashMap<>();
    private final TableServiceFactory tableServiceFactory;

    public TableManagerImpl(TableServiceFactory tableServiceFactory) {
        this.tableServiceFactory = tableServiceFactory;
    }

    @Override
    public void create(String tableName) {
        map.putIfAbsent(tableName, createServiceHolder(tableName));
    }

    private ServiceHolder createServiceHolder(String tableName) {
        tableServiceFactory.createServices(tableName);
        final ModelService modelService = tableServiceFactory.getService(tableName, ModelService.class);
        final RowRepository rowRepository = tableServiceFactory.getService(tableName, RowRepository.class);
        return new ServiceHolder(rowRepository, modelService);
    }

    @Override
    public void delete(String tableName) {
        map.remove(tableName);
    }

    @Override
    public ServiceHolder getServiceHolder(String tableName) {
        return map.get(tableName);
    }

}
