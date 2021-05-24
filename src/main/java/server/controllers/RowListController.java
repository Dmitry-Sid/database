package server.controllers;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Controller
@RequestMapping(value = "/", produces = "text/plain;charset=UTF-8")
public class RowListController {
    private static final Logger log = LoggerFactory.getLogger(RowListController.class);
    private static final int ROWS_PER_PAGE = 25;
    private static final int MAX_SIZE = 500;
    private final RowRepository rowRepository;
    private final ModelService modelService;
    private final ConditionService conditionService;

    public RowListController(RowRepository rowRepository, ModelService modelService, ConditionService conditionService) {
        this.rowRepository = rowRepository;
        this.modelService = modelService;
        this.conditionService = conditionService;
        log.info("created info");
        log.debug("created debug");
        log.error("created error");
        log.warn("created warn");
    }

    @GetMapping("/")
    public String searchRows(Model model, HttpServletRequest request, @RequestParam(defaultValue = "") String searchRequest, @RequestParam(defaultValue = "1") int page) throws UnsupportedEncodingException {
        long start = System.currentTimeMillis();
        if (StringUtils.isBlank(searchRequest)) {
            searchRequest = (String) request.getSession().getAttribute("searchRequest");
            if (StringUtils.isNotBlank(searchRequest)) {
                return "redirect:/?searchRequest=" + URLEncoder.encode(searchRequest, "UTF-8");
            }
        }
        model.addAttribute("searchRequest", searchRequest);
        model.addAttribute("page", page);
        try {
            final ICondition condition;
            final int totalPages;
            if (request.getSession().getAttribute("totalPages:" + searchRequest) != null) {
                condition = null;
                totalPages = (int) request.getSession().getAttribute("totalPages:" + searchRequest);
            } else {
                condition = conditionService.parse(searchRequest);
                final int pages = (int) Math.ceil((double) rowRepository.size(condition, MAX_SIZE) / ROWS_PER_PAGE);
                totalPages = pages == 0 ? 1 : pages;
                request.getSession().setAttribute("totalPages:" + searchRequest, totalPages);
            }
            model.addAttribute("totalPages", totalPages);
            model.addAttribute("pageNumbers", IntStream.rangeClosed(1, totalPages).boxed().collect(Collectors.toList()));
            final List<Row> rows;
            if (request.getSession().getAttribute("rows:" + searchRequest) != null) {
                rows = (List<Row>) request.getSession().getAttribute("rows:" + searchRequest);
            } else {
                rows = rowRepository.getList(condition, 0, ROWS_PER_PAGE * totalPages);
                request.getSession().setAttribute("rows:" + searchRequest, rows);
            }
            model.addAttribute("rows", rows.size() > 0 ? rows.subList((page - 1) * ROWS_PER_PAGE, Math.min(rows.size(), page * ROWS_PER_PAGE)) : rows);
            model.addAttribute("fields", modelService.getFields());
        } catch (ConditionException e) {
        }
        log.info("time " + (System.currentTimeMillis() - start));
        return "index";
    }

    @PostMapping("/")
    public String setSearchRequest(@RequestParam String searchRequest, HttpServletRequest request) throws UnsupportedEncodingException {
        request.getSession().setAttribute("searchRequest", searchRequest);
        request.getSession().setAttribute("totalPages:" + searchRequest, null);
        request.getSession().setAttribute("rows:" + searchRequest, null);
        return "redirect:/?searchRequest=" + URLEncoder.encode(searchRequest, "UTF-8");
    }

    @GetMapping("/delete")
    public String delete(@RequestParam int id) {
        rowRepository.delete(id);
        return "redirect:/";
    }
}
