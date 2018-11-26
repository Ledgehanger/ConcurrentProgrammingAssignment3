class SQLUpdateStatement extends SQLStatement {
    private final SQLField field;
    private final DataValue value;
    private final SQLTable table;
    private final SQLWhereClause where_clause;

    SQLUpdateStatement( SQLField field_,
			DataValue value_,
			SQLTable table_,
			SQLWhereClause where_clause_
			) {
	field = field_;
	value = value_;
	table = table_;
	where_clause = where_clause_;
	// group_by = null;
    }

    public SQLField getField() { return field; }
    public DataValue getValue() { return value; }
    public SQLTable getTable() { return table; }
    public SQLWhereClause getWhereClause() { return where_clause; }
}
