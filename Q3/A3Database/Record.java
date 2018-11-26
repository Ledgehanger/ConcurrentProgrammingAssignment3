import java.util.ArrayList;

class Record {
    private ArrayList<DataValue> data;

    Record( final int estimated_length ) {
	data = new ArrayList<DataValue>( estimated_length );
    }

    public boolean add( DataValue val ) {
	data.add( val );
	return true;
    }
    public boolean add( int position, DataValue val ) {
	data.add( position, val );
	return true;
    }
    public boolean set( int position, DataValue val ) {
	data.set( position, val );
	return true;
    }
    public DataValue get( int position ) {
	return data.get( position );
    }
    public void print() {
	for( DataValue f : data ) {
	    f.print();
	    System.out.print( " " );
	}
	System.out.println();
    }

    public String toString() {
	String s = new String();
	for( DataValue val : data )
	    s = s + val.toString() + " : ";
	return s;
    }

}
