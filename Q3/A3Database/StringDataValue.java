class StringDataValue extends DataValue implements Comparable<DataValue> {
    private String value;

    StringDataValue( String value_ ) { value = value_; }

    public void print() {
	System.err.print( "SDV:" + value );
    }

    public int compareTo( DataValue v ) {
	if( v instanceof StringDataValue ) {
	    return value.compareTo( ((StringDataValue)v).value );
	} else {
	    // This should never happen
	    return -1;
	}
    }

    public boolean equals( DataValue v ) {
	if( v instanceof StringDataValue ) {
	    return value.equals( ((StringDataValue)v).value );
	} else {
	    // This should never happen
	    return false;
	}
    }

    public DataValue construct( String v ) {
	return new StringDataValue( v );
    }

    public int toInteger() {
	return Integer.parseInt( value );
    }

    public String toString() {
	return "SDV:" + value;
    }
}
