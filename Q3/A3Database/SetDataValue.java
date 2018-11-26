import java.util.ArrayList;
import java.util.Iterator;

class SetDataValue extends DataValue
    implements Comparable<DataValue>, Iterable<DataValue> {
    ArrayList<DataValue> list;
    
    SetDataValue( DataValue v0 ) {
	list = new ArrayList<DataValue>();
	list.add( v0 );
    }
    SetDataValue( DataValue v0, DataValue v1 ) {
	list = new ArrayList<DataValue>();
	list.add( v0 );
	list.add( v1 );
    }

    public void print() { System.out.println( toString() ); }

    public int compareTo( DataValue v ) {
	// Should not call this
	return -1;
    }

    public DataValue construct( String v ) {
	return new StringDataValue( "error" );
    }

    public int toInteger() { return -1; }

    public String toString() {
	String s = "";
	for( DataValue v : list )
	    s += ", " + v.toString();
	return s;
    }

    public Iterator<DataValue> iterator() {
	return list.iterator();
    }
}
