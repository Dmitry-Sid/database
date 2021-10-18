package server.model.impl;

import server.model.*;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableManagerImpl extends BaseDestroyable implements TableManager {
    private static final String FILE_NAME = "tables";

    private final Map<String, ServiceHolder> map = new ConcurrentHashMap<>();
    private final TableServiceFactory tableServiceFactory;
    private final String tablesFileName;
    private volatile boolean changed;

    public TableManagerImpl(String filePath, boolean init, ObjectConverter objectConverter, DestroyService destroyService, TableServiceFactory tableServiceFactory) {
        super(filePath, init, objectConverter, destroyService);
        this.tableServiceFactory = tableServiceFactory;
        this.tablesFileName = filePath + FILE_NAME;
        if (new File(tablesFileName).exists()) {
            objectConverter.fromFile(HashSet.class, this.tablesFileName).forEach(table -> create((String) table));
        }
    }

    @Override
    public void create(String tableName) {
        map.putIfAbsent(tableName, createServiceHolder(tableName));
        changed = true;
    }

    private ServiceHolder createServiceHolder(String tableName) {
        final ServiceHolder[] serviceHolders = new ServiceHolder[1];
        tableServiceFactory.createServices(tableName, map -> {
            serviceHolders[0] = new ServiceHolder((RowRepository) map.get(RowRepository.class), (ModelService) map.get(ModelService.class));
        }, ModelService.class, RowRepository.class);
        return serviceHolders[0];
    }

    @Override
    public void delete(String tableName) {
        map.remove(tableName);
        changed = true;
    }

    @Override
    public ServiceHolder getServiceHolder(String tableName) {
        return map.get(tableName);
    }

    @Override
    public Set<String> getTables() {
        return map.keySet();
    }

    @Override
    public void destroy() {
        if (changed) {
            objectConverter.toFile(new HashSet<>(map.keySet()), tablesFileName);
            changed = false;
        }
    }
}
