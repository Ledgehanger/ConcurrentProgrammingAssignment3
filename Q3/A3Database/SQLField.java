class SQLField {
    private final String field_name;
    private final int aggregate;

    SQLField( final String field_name_, final int aggregate_ ) {
	field_name = field_name_;
	aggregate = aggregate_;
    }
    SQLField( final String field_name_ ) {
	this( field_name_, ResultSet.AGGREGATE_APPEND );
    }

    public String getName() { return field_name; }
    public boolean isSummaryField() {
	return aggregate != ResultSet.AGGREGATE_APPEND;
    }
    public int getAggregation() { return aggregate; }
};
