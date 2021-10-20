package server.controllers;

import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import server.model.ModelService;
import server.model.RowRepository;
import server.model.TableManager;
import server.model.pojo.PersistentFields;
import server.model.pojo.Row;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

@RequestMapping(value = "/row", produces = "text/plain;charset=UTF-8")
public class RowController extends BaseController {

    public RowController(TableManager tableManager, PersistentFields persistentFields) {
        super(tableManager, persistentFields);
    }

    @GetMapping
    public String getRow(Model model, @RequestParam(defaultValue = "0") int id) {
        final TableManager.ServiceHolder serviceHolder = tableManager.getServiceHolder(persistentFields.getTableName());
        final RowRepository rowRepository = serviceHolder.rowRepository;
        final ModelService modelService = serviceHolder.modelService;
        final Row[] row = new Row[1];
        if (id == 0) {
            row[0] = new Row(0, new HashMap<>());
        } else {
            rowRepository.process(id, processedRow -> {
                row[0] = processedRow;
            });
        }
        setCommonAttributes(model, row[0], modelService);
        return "row";
    }

    @PostMapping
    public String save(Model model, @ModelAttribute Row row) throws UnsupportedEncodingException {
        final TableManager.ServiceHolder serviceHolder = tableManager.getServiceHolder(persistentFields.getTableName());
        final RowRepository rowRepository = serviceHolder.rowRepository;
        final ModelService modelService = serviceHolder.modelService;
        final Map<String, Comparable> map = new HashMap<>();
        row.getFields().forEach((key, value) -> map.put(key, modelService.getValue(key, (String) value)));
        map.forEach((key, value) -> row.getFields().put(key, value));
        rowRepository.add(row);
        setCommonAttributes(model, row, modelService);
        return "row";
    }

    private void setCommonAttributes(Model model, Row row, ModelService modelService) {
        model.addAttribute("row", row);
        model.addAttribute("fields", modelService.getFields());
    }
}
