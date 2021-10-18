package server.controllers;

import org.apache.commons.lang3.StringUtils;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import server.model.ModelService;
import server.model.TableManager;

import java.util.ArrayList;
import java.util.List;

@RequestMapping(value = "/model", produces = "text/plain;charset=UTF-8")
public class ModelController extends BaseController {

    public ModelController(TableManager tableManager) {
        super(tableManager);
    }

    @GetMapping("/")
    public String getFields(Model model) {
        final ModelService modelService = tableManager.getServiceHolder(RowListController.tableName).modelService;
        model.addAttribute("types", modelService.types);
        model.addAttribute("fieldsForm", new FieldsForm(modelService.getFields()));
        return "model";
    }

    @GetMapping("/delete")
    public String delete(@RequestParam String field) {
        tableManager.getServiceHolder(RowListController.tableName).modelService.delete(field);
        return "redirect:/model/";
    }

    @PostMapping("/")
    public String saveFields(@ModelAttribute FieldsForm fieldsForm) {
        final List<String> deletedIndexes = new ArrayList<>();
        final List<String> addedIndexes = new ArrayList<>();
        for (ModelService.FieldInfo fieldInfo : fieldsForm.fields) {
            if (!fieldInfo.isIndex()) {
                deletedIndexes.add(fieldInfo.getName());
            } else {
                addedIndexes.add(fieldInfo.getName());
            }
        }
        final ModelService modelService = tableManager.getServiceHolder(RowListController.tableName).modelService;
        modelService.deleteIndex(deletedIndexes.toArray(new String[0]));
        modelService.addIndex(addedIndexes.toArray(new String[0]));
        return "redirect:/model/";
    }

    @PostMapping("/add")
    public String addField(@RequestParam String fieldName, @RequestParam String type) throws ClassNotFoundException {
        if (StringUtils.isNotBlank(fieldName) && StringUtils.isNotBlank(type)) {
            tableManager.getServiceHolder(RowListController.tableName).modelService.add(fieldName, Class.forName(type));
        }
        return "redirect:/model/";
    }

    public static class FieldsForm {
        private List<ModelService.FieldInfo> fields;

        public FieldsForm(List<ModelService.FieldInfo> fields) {
            this.fields = fields;
        }

        public List<ModelService.FieldInfo> getFields() {
            return fields;
        }

        public void setFields(List<ModelService.FieldInfo> fields) {
            this.fields = fields;
        }
    }
}
