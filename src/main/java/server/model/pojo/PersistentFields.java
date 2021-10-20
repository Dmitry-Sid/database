package server.model.pojo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersistentFields {
    private final Map<String, TableFields> tableFieldsMap = new HashMap<>();
    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TableFields getTableFields() {
        return tableFieldsMap.get(tableName);
    }

    public void setTableFields(TableFields tableFields) {
        tableFieldsMap.put(tableName, tableFields);
    }

    public static class TableFields {
        private String searchRequest;
        private Integer totalPages;
        private List<Row> rows;

        public TableFields(String searchRequest, Integer totalPages, List<Row> rows) {
            this.searchRequest = searchRequest;
            this.totalPages = totalPages;
            this.rows = rows;
        }

        public String getSearchRequest() {
            return searchRequest;
        }

        public void setSearchRequest(String searchRequest) {
            this.searchRequest = searchRequest;
        }

        public Integer getTotalPages() {
            return totalPages;
        }

        public void setTotalPages(Integer totalPages) {
            this.totalPages = totalPages;
        }

        public List<Row> getRows() {
            return rows;
        }

        public void setRows(List<Row> rows) {
            this.rows = rows;
        }
    }
}
