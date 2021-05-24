package server.controllers;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import server.model.ModelService;

import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping(value = "/model", produces = "text/plain;charset=UTF-8")
public class ModelController {
    private final ModelService modelService;

    public ModelController(ModelService modelService) {
        this.modelService = modelService;
    }

    @GetMapping("/")
    public String getFields(Model model) {
        model.addAttribute("types", modelService.types);
        model.addAttribute("fieldsForm", new FieldsForm(modelService.getFields()));
        return "model";
    }

    @GetMapping("/delete")
    public String delete(@RequestParam String field) {
        modelService.delete(field);
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
        modelService.deleteIndex(deletedIndexes.toArray(new String[0]));
        modelService.addIndex(addedIndexes.toArray(new String[0]));
        return "redirect:/model/";
    }

    @PostMapping("/add")
    public String addField(@RequestParam String fieldName, @RequestParam String type) throws ClassNotFoundException {
        if (StringUtils.isNotBlank(fieldName) && StringUtils.isNotBlank(type)) {
            modelService.add(fieldName, Class.forName(type));
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
