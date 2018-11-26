class RecordVisitorEvaluateWhere implements RecordVisitor {
    private RecordVisitor method;
    private SQLWhereClause clause;
    private Table table;

    RecordVisitorEvaluateWhere( Table table_, RecordVisitor method_,
				SQLWhereClause clause_ ) {
	table = table_;
	method = method_;
	clause = clause_;
    }
    
    public void visit( Record row ) {
	// See if the row matches the WHERE clause.
	if( table.matches( row, clause ) )
	    method.visit( row );
    }
}
