package server.controllers;

import org.apache.commons.lang3.StringUtils;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import server.model.ModelService;
import server.model.TableManager;
import server.model.pojo.PersistentFields;

import java.util.ArrayList;
import java.util.List;

@RequestMapping(value = "/model", produces = "text/plain;charset=UTF-8")
public class ModelController extends BaseController {

    public ModelController(TableManager tableManager, PersistentFields persistentFields) {
        super(tableManager, persistentFields);
    }

    @GetMapping
    public String getFields(Model model) {
        setCommonAttributes(model, tableManager.getServiceHolder(persistentFields.getTableName()).modelService);
        return "model";
    }

    @GetMapping("/delete")
    public String delete(Model model, @RequestParam String field) {
        final ModelService modelService = tableManager.getServiceHolder(persistentFields.getTableName()).modelService;
        modelService.delete(field);
        setCommonAttributes(model, modelService);
        return "model";
    }

    @PostMapping
    public String saveFields(Model model, @ModelAttribute FieldsForm fieldsForm) {
        final List<String> deletedIndexes = new ArrayList<>();
        final List<String> addedIndexes = new ArrayList<>();
        for (ModelService.FieldInfo fieldInfo : fieldsForm.fields) {
            if (!fieldInfo.isIndex()) {
                deletedIndexes.add(fieldInfo.getName());
            } else {
                addedIndexes.add(fieldInfo.getName());
            }
        }
        final ModelService modelService = tableManager.getServiceHolder(persistentFields.getTableName()).modelService;
        modelService.deleteIndex(deletedIndexes.toArray(new String[0]));
        modelService.addIndex(addedIndexes.toArray(new String[0]));
        setCommonAttributes(model, modelService);
        return "model";
    }

    @PostMapping("/add")
    public String addField(Model model, @RequestParam String fieldName, @RequestParam String type) throws ClassNotFoundException {
        final ModelService modelService = tableManager.getServiceHolder(persistentFields.getTableName()).modelService;
        if (StringUtils.isNotBlank(fieldName) && StringUtils.isNotBlank(type)) {
            modelService.add(fieldName, Class.forName(type));
        }
        setCommonAttributes(model, modelService);
        return "model";
    }

    private void setCommonAttributes(Model model, ModelService modelService) {
        model.addAttribute("types", modelService.types);
        model.addAttribute("fieldsForm", new FieldsForm(modelService.getFields()));
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
