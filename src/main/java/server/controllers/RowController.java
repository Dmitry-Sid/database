package server.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import server.model.ModelService;
import server.model.RowRepository;
import server.model.pojo.Row;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping(value = "/row", produces = "text/plain;charset=UTF-8")
public class RowController {
    private final RowRepository rowRepository;
    private final ModelService modelService;

    public RowController(RowRepository rowRepository, ModelService modelService) {
        this.rowRepository = rowRepository;
        this.modelService = modelService;
    }

    @GetMapping("/")
    public String getRow(Model model, @RequestParam(defaultValue = "0") int id) {
        final Row[] row = new Row[1];
        if (id == 0) {
            row[0] = new Row(0, new HashMap<>());
        } else {
            rowRepository.process(id, processedRow -> {
                row[0] = processedRow;
            });
        }
        model.addAttribute("row", row[0]);
        model.addAttribute("fields", modelService.getFields());
        return "row";
    }

    @PostMapping("/")
    public String save(@ModelAttribute Row row) throws UnsupportedEncodingException {
        final Map<String, Comparable> map = new HashMap<>();
        row.getFields().forEach((key, value) -> {
            map.put(key, modelService.getValue(key, (String) value));
        });
        map.forEach((key, value) -> row.getFields().put(key, value));
        rowRepository.add(row);
        return "redirect:/row/?id=" + URLEncoder.encode(Integer.toString(row.getId()), "UTF-8");
    }
}
