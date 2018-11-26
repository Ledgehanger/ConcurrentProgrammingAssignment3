class RecordVisitorUpdate implements RecordVisitor {
    private Table table;
    private SQLField field;
    private DataValue value;
    private ResultSet result;

    RecordVisitorUpdate( Table table_, SQLField field_, DataValue value_,
			 ResultSet result_ ) {
	table = table_;
	field = field_;
	value = value_;
	result = result_;
    }
    
    public void visit( Record row ) {
	if( table.update( row, field, value ) )
	    result.add( row );
	else
	    result.set( false ); // indicate failure
    }
}
