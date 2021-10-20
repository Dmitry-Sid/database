package server.controllers;

import server.model.TableManager;
import server.model.pojo.PersistentFields;

public abstract class BaseController {
    protected final TableManager tableManager;
    protected final PersistentFields persistentFields;

    public BaseController(TableManager tableManager, PersistentFields persistentFields) {
        this.tableManager = tableManager;
        this.persistentFields = persistentFields;
    }
}
