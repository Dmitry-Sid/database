package server.model.impl;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableManagerImpl extends BaseDestroyable implements TableManager {
    private static final Logger log = LoggerFactory.getLogger(TableManagerImpl.class);
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
            serviceHolders[0] = new ServiceHolder((RowRepository) map.get(RowRepository.class), (ModelService) map.get(ModelService.class), (ConditionService) map.get(ConditionService.class));
        }, ModelService.class, RowRepository.class, ConditionService.class);
        return serviceHolders[0];
    }

    @Override
    public void delete(String tableName) {
        final ServiceHolder serviceHolder = map.remove(tableName);
        if (serviceHolder != null) {
            serviceHolder.rowRepository.stop();
            new Thread(() -> {
                try {
                    Thread.sleep(destroyService.getSleepTime());
                } catch (InterruptedException e) {
                    log.error("sleep error", e);
                }
                try {
                    FileUtils.deleteDirectory(new File(Utils.getFullPath(filePath, tableName)));
                } catch (IOException e) {
                    log.error("cannot delete directory", e);
                }
            }).start();
        }
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
