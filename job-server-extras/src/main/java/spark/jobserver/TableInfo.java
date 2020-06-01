package spark.jobserver;

public class TableInfo {
    private String tableName;
    private String sqlFile;
    private String tempViewName;

    public TableInfo() {
    }

    public TableInfo(String tableName, String sqlFile, String tempViewName) {
        this.tableName = tableName;
        this.sqlFile = sqlFile;
        this.tempViewName = tempViewName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSqlFile() {
        return sqlFile;
    }

    public void setSqlFile(String sqlFile) {
        this.sqlFile = sqlFile;
    }

    public String getTempViewName() {
        return tempViewName;
    }

    public void setTempViewName(String tempViewName) {
        this.tempViewName = tempViewName;
    }
}
