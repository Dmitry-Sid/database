package server.controllers;

import server.model.TableManager;

public abstract class BaseController {
    protected final TableManager tableManager;

    public BaseController(TableManager tableManager) {
        this.tableManager = tableManager;
    }
}
