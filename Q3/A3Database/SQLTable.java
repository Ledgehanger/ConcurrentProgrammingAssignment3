class SQLTable {
    private final String table_name;

    SQLTable( final String table_name_ ) {
	table_name = table_name_;
    }

    public String getTableName() { return table_name; }
}
