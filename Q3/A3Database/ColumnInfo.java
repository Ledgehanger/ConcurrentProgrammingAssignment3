class ColumnInfo {
    public static final int IntegerType = 1;
    public static final int StringType = 2;

    private int column_number;
    private boolean is_key;
    private int type;
    private String name;

    ColumnInfo( int n, boolean k, int t, String name_ ) {
	column_number = n;
	is_key = k;
	type = t;
	name = name_;
    }

    public String getName() { return name; }
    public int getType() { return type; }
    public int getPosition() { return column_number; }
    public boolean isKey() { return is_key; }

    public DataValue convert( final String value ) {
	switch( type ) {
	case IntegerType:
	    return new IntegerDataValue( Integer.parseInt( value ) );
	case StringType:
	    return new StringDataValue( value );
	default:
	    assert false : "error";
	    return new IntegerDataValue( -1 );
	}
    }
}
