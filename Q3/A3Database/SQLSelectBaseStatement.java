class SQLSelectBaseStatement extends SQLStatement {
    private final SQLFieldList field_list;
    private final SQLTable table;
    private final SQLWhereClause where_clause;

    SQLSelectBaseStatement( SQLFieldList field_list_,
			    SQLTable table_,
			    SQLWhereClause where_clause_
			    ) {
	field_list = field_list_;
	table = table_;
	where_clause = where_clause_;
	// group_by = null;
    }

    public SQLFieldList getFieldList() { return field_list; }
    public SQLTable getTable() { return table; }
    public SQLWhereClause getWhereClause() { return where_clause; }
    public boolean hasGroupByClause() { return false; } // group_by != null; }
}
