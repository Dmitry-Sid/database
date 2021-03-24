package server.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import server.model.ConditionException;
import server.model.ConditionService;
import server.model.ModelService;
import server.model.RowRepository;
import server.model.pojo.ICondition;
import server.model.pojo.Row;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping(value = "/", produces = "text/plain;charset=UTF-8")
public class RowListController {
    private static final int ROWS_PER_PAGE = 25;
    private final RowRepository rowRepository;
    private final ModelService modelService;
    private final ConditionService conditionService;

    public RowListController(RowRepository rowRepository, ModelService modelService, ConditionService conditionService) {
        this.rowRepository = rowRepository;
        this.modelService = modelService;
        this.conditionService = conditionService;
    }

    @GetMapping("/")
    public String getCityPlaces(Model model, @RequestParam(defaultValue = "") String searchRequest, @RequestParam(defaultValue = "0") int page) {
        model.addAttribute("searchRequest", searchRequest);
        try {
            model.addAttribute("rows", rowRepository.getList(conditionService.parse(searchRequest), page * ROWS_PER_PAGE, ROWS_PER_PAGE));
            model.addAttribute("fields", modelService.getFields());
        } catch (ConditionException e) {
        }
        return "index";
    }

    @PostMapping("/")
    public String searchPlacesByCity(@RequestParam String searchRequest) throws UnsupportedEncodingException {
        return "redirect:/?searchRequest=" + URLEncoder.encode(searchRequest, "UTF-8");
    }
}
