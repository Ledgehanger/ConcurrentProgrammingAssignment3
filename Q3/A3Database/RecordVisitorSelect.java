class RecordVisitorSelect implements RecordVisitor {
    private ResultSet result;
    private SQLFieldList fields;
    private Table table;

    RecordVisitorSelect( Table table_, ResultSet result_, SQLFieldList fields_ ) {
	table = table_;
	result = result_;
	fields = fields_;
    }
    
    public void visit( Record record ) {
	result.add( table.extract( record, fields ) );
    }
}
