package server.controllers;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import server.model.TableManager;
import server.model.pojo.PersistentFields;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

@RequestMapping(value = "/tablesManager", produces = "text/plain;charset=UTF-8")
public class TablesController extends BaseController {

    public TablesController(TableManager tableManager, PersistentFields persistentFields) {
        super(tableManager, persistentFields);
    }

    @GetMapping
    public String getTables() {
        return "tables";
    }

    @GetMapping("/delete")
    public String delete(@RequestParam String tableName, HttpServletRequest request) {
        if (!tableManager.getTables().contains(tableName)) {
            ServletUtils.makeError(request, "table " + tableName + " doesn't exist");
        } else {
            tableManager.delete(tableName);
            if (tableName.equals(persistentFields.getTableName())) {
                persistentFields.setTableName(null);
            }
        }
        return "tables";
    }

    @PostMapping("/add")
    public String add(@RequestParam String tableName, HttpServletRequest request) {
        if (StringUtils.isBlank(tableName)) {
            ServletUtils.makeError(request, "table name is empty");
        } else {
            if (tableManager.getTables().contains(tableName)) {
                ServletUtils.makeError(request, "table already exists");
            } else {
                tableManager.create(tableName);
                persistentFields.setTableName(tableName);
            }
        }
        return "tables";
    }

    @GetMapping("/setTable")
    public String setTable(@RequestParam String redirectURI, @RequestParam String tableName, HttpServletRequest request) throws UnsupportedEncodingException {
        persistentFields.setTableName(tableName);
        return "redirect:" + URLDecoder.decode(redirectURI, "UTF-8").substring(request.getContextPath().length());
    }
}