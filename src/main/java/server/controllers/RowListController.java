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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    public String searchRows(Model model, @RequestParam(defaultValue = "") String searchRequest, @RequestParam(defaultValue = "1") int page) {
        model.addAttribute("searchRequest", searchRequest);
        model.addAttribute("page", page);
        try {
            final ICondition condition = conditionService.parse(searchRequest);
            final int totalPages = (int) Math.ceil((double) rowRepository.size(condition) / ROWS_PER_PAGE);
            model.addAttribute("totalPages", totalPages);
            model.addAttribute("pageNumbers", IntStream.rangeClosed(1, totalPages)
                    .boxed().collect(Collectors.toList()));
            model.addAttribute("rows", rowRepository.getList(condition, (page - 1) * ROWS_PER_PAGE, ROWS_PER_PAGE));
            model.addAttribute("fields", modelService.getFields());
        } catch (ConditionException e) {
        }
        return "index";
    }

    @PostMapping("/")
    public String setSearchRequest(@RequestParam String searchRequest) throws UnsupportedEncodingException {
        return "redirect:/?searchRequest=" + URLEncoder.encode(searchRequest, "UTF-8");
    }

    @GetMapping("/delete")
    public String delete(@RequestParam int id) {
        rowRepository.delete(id);
        return "redirect:/";
    }
}
