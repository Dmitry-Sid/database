package server.controllers;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import server.model.*;
import server.model.pojo.ICondition;
import server.model.pojo.PersistentFields;
import server.model.pojo.Row;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequestMapping(value = "/search", produces = "text/plain;charset=UTF-8")
public class RowListController extends BaseController {
    private static final Logger log = LoggerFactory.getLogger(RowListController.class);
    private static final int ROWS_PER_PAGE = 25;
    private static final int MAX_SIZE = 500;

    public RowListController(TableManager tableManager, PersistentFields persistentFields) {
        super(tableManager, persistentFields);
        /*for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100_000; j++) {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("firstName", "firstName" + j);
                map.put("lastName", "lastName" + j);
                map.put("age", j);
                map.put("money", j + 0.5);
                tableManager.getServiceHolder("test").rowRepository.add(new Row(0, map));
            }
        }*/
    }

    @GetMapping
    public String searchRows(Model model, HttpServletRequest request, @RequestParam(defaultValue = "") String searchRequest, @RequestParam(defaultValue = "1") int page) throws UnsupportedEncodingException {
        final TableManager.ServiceHolder serviceHolder = tableManager.getServiceHolder(persistentFields.getTableName());
        final RowRepository rowRepository = serviceHolder.rowRepository;
        final ModelService modelService = serviceHolder.modelService;
        final ConditionService conditionService = serviceHolder.conditionService;
        if (persistentFields.getTableFields() == null) {
            persistentFields.setTableFields(new PersistentFields.TableFields(null, null, null));
        }
        final PersistentFields.TableFields tableFields = persistentFields.getTableFields();
        long start = System.currentTimeMillis();
        if (StringUtils.isBlank(searchRequest)) {
            searchRequest = tableFields.getSearchRequest();
            if (StringUtils.isNotBlank(searchRequest)) {
                return "redirect:/search?searchRequest=" + URLEncoder.encode(searchRequest, "UTF-8");
            }
        }
        model.addAttribute("searchRequest", searchRequest);
        model.addAttribute("page", page);
        try {
            final ICondition condition;
            final int totalPages;
            if (tableFields.getTotalPages() != null) {
                condition = null;
                totalPages = tableFields.getTotalPages();
            } else {
                condition = conditionService.parse(searchRequest);
                final int pages = (int) Math.ceil((double) rowRepository.size(condition, MAX_SIZE) / ROWS_PER_PAGE);
                totalPages = pages == 0 ? 1 : pages;
                tableFields.setTotalPages(totalPages);
            }
            model.addAttribute("totalPages", totalPages);
            model.addAttribute("pageNumbers", IntStream.rangeClosed(1, totalPages).boxed().collect(Collectors.toList()));
            final List<Row> rows;
            if (tableFields.getRows() != null) {
                rows = tableFields.getRows();
            } else {
                rows = rowRepository.getList(condition, 0, ROWS_PER_PAGE * totalPages);
                tableFields.setRows(rows);
            }
            model.addAttribute("rows", rows.size() > 0 ? rows.subList((page - 1) * ROWS_PER_PAGE, Math.min(rows.size(), page * ROWS_PER_PAGE)) : rows);
            model.addAttribute("fields", modelService.getFields());
        } catch (ConditionException e) {
            ServletUtils.makeError(request, e.getMessage());
        }
        log.info("search time " + (System.currentTimeMillis() - start));
        return "search";
    }

    @GetMapping("/delete")
    public String deleteRow(int id) throws UnsupportedEncodingException {
        tableManager.getServiceHolder(persistentFields.getTableName()).rowRepository.delete(id);
        persistentFields.getTableFields().getRows().removeIf(row -> id == row.getId());
        return "redirect:/search?searchRequest=" + URLEncoder.encode(StringUtils.trimToEmpty(persistentFields.getTableFields().getSearchRequest()), "UTF-8");
    }

    @PostMapping
    public String setSearchRequest(@RequestParam String searchRequest) throws UnsupportedEncodingException {
        persistentFields.setTableFields(new PersistentFields.TableFields(searchRequest, null, null));
        return "redirect:/search?searchRequest=" + URLEncoder.encode(searchRequest, "UTF-8");
    }
}
