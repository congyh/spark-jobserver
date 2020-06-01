package spark.jobserver;

public class LoadTableConfig {

    private TableInfo[] tables;

    public LoadTableConfig() {
    }

    public LoadTableConfig(TableInfo[] tables) {
        this.tables = tables;
    }

    public TableInfo[] getTables() {
        return tables;
    }

    public void setTables(TableInfo[] tables) {
        this.tables = tables;
    }
}
