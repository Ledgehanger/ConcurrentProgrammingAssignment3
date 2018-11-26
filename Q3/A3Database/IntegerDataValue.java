class IntegerDataValue extends DataValue implements Comparable<DataValue> {
    private int value;

    IntegerDataValue( int value_ ) { value = value_; }

    public void set( int v ) { value = v; }

    public void print() {
	System.err.print( "IDV:" + value );
    }

    public int compareTo( DataValue v ) {
	// System.err.println( "IDV compare " + v + " " + this );
	if( v instanceof IntegerDataValue ) {
	    return value - ((IntegerDataValue)v).value;
	} else {
	    // This should never happen
	    return -1;
	}
    }

    public boolean equals( DataValue v ) {
	// System.err.println( "IDV equals " + v + " " + this );
	if( v instanceof IntegerDataValue ) {
	    return value == ((IntegerDataValue)v).value;
	} else {
	    // This should never happen
	    return false;
	}
    }

    public DataValue construct( String v ) {
	return new IntegerDataValue( Integer.parseInt( v ) );
    }

    public int toInteger() {
	return value;
    }

    public String toString() {
	return "IDV:" + Integer.toString( value );
    }
}
