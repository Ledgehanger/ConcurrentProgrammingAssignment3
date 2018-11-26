import java.lang.Iterable;
import java.util.Iterator;
import java.util.ArrayList;

class SQLValueList implements Iterable<DataValue> {
    private ArrayList<DataValue> values;

    SQLValueList( int estimated_size ) {
	values = new ArrayList<DataValue>( estimated_size );
    }

    public void addValue( DataValue f ) {
	values.add( f );
    }

    public DataValue get( int pos ) {
	return values.get( pos );
    }

    public DataValue set( int pos, DataValue val ) {
	return values.set( pos, val );
    }

    public int size() { return values.size(); }

    public Iterator<DataValue> iterator() {
	return values.iterator();
    }
}
