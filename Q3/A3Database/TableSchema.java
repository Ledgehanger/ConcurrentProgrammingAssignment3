import java.lang.Iterable;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.ListIterator;

class TableSchema implements Iterable<ColumnInfo> {
    private ArrayList<ColumnInfo> columns;
    private Map<String,ColumnInfo> lookup;

    TableSchema() {
	columns = new ArrayList<ColumnInfo>();
	lookup = new TreeMap<String,ColumnInfo>();
    }

    public void addColumn( int no, boolean is_key, int type,
			   String name ) {
	ColumnInfo col = new ColumnInfo( no, is_key, type, name );
	columns.add( no, col );
	lookup.put( name, col );
    }

    public boolean isKey( final String field_name ) {
	return lookup.get( field_name ).isKey();
    }

    public int size() {
	return columns.size();
    }

    public int getType( final String field_name ) {
	return lookup.get( field_name ).getType();
    }

    public ColumnInfo getColumnInfo( final String field_name ) {
	return lookup.get( field_name );
    }

    public ColumnInfo getKey() {
	for( ColumnInfo c : columns ) {
	    if( c.isKey() )
		return c;
	}
	return null;
    }

    public ListIterator<ColumnInfo> iterator() {
	return columns.listIterator();
    }

    public Record assembleRecord( SQLFieldList fields, SQLValueList values ) {
	Record row = new Record( size() );
	
	for( int i=0; i < columns.size(); ++i )
	    row.add( null );
	
	int pos = 0;
	for( SQLField field : fields ) {
	    ColumnInfo ci = getColumnInfo( field.getName() );
	    row.set( ci.getPosition(), values.get( pos ) );
	    ++pos;
	}

	// Have all fields been set?
	// Note: we don't support default values.
	// Note: this assumes all fields in the query are distinct
	return fields.size() == this.size() ? row : null;
    }
}
