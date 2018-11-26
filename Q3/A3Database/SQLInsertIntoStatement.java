class SQLInsertIntoStatement extends SQLStatement {
    private final SQLFieldList fields;
    private final SQLValueList values;
    private final SQLTable table;

    SQLInsertIntoStatement( SQLTable table_,
			    SQLFieldList fields_,
			    SQLValueList values_
			) {
	table = table_;
	fields = fields_;
	values = values_;
    }

    public SQLTable getTable() { return table; }
    public SQLFieldList getFields() { return fields; }
    public SQLValueList getValues() { return values; }
}
