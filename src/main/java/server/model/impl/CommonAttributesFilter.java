package server.model.impl;

import server.model.TableManager;
import server.model.pojo.PersistentFields;

import javax.servlet.*;
import java.io.IOException;
import java.util.ArrayList;

public class CommonAttributesFilter implements Filter {

    private final TableManager tableManager;
    private final PersistentFields persistentFields;

    public CommonAttributesFilter(TableManager tableManager, PersistentFields persistentFields) {
        this.tableManager = tableManager;
        this.persistentFields = persistentFields;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        servletRequest.setAttribute("tableManager", tableManager);
        if (persistentFields.getTableName() == null && !tableManager.getTables().isEmpty()) {
            persistentFields.setTableName(new ArrayList<>(tableManager.getTables()).get(0));
        }
        servletRequest.setAttribute("persistentFields", persistentFields);
        filterChain.doFilter(servletRequest, servletResponse);
    }
}