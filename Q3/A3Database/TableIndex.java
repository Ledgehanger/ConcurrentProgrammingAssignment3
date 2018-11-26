import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

// A primary index for a table. It is assumed that the primary index has
// unique keys: there can be at most one record for each key.
class TableIndex {
    private final Map<DataValue,Record> index;
    private final ColumnInfo primary_key;

    TableIndex( ColumnInfo primary_key_ ) {
	index = new TreeMap<DataValue,Record>();
	primary_key = primary_key_;
    }

    public void visitAll( RecordVisitor method ) {
	Iterator<Map.Entry<DataValue,Record>> it
	    = index.entrySet().iterator();
	while( it.hasNext() )
	    method.visit( it.next().getValue() );
    }
    public void visitOne( DataValue value, RecordVisitor method ) {
	Record row = index.get( value );
	method.visit( row );
    }

    public boolean insert( Record row, TableSchema schema ) {
	DataValue key = row.get( primary_key.getPosition() );
	Record old = index.put( key, row );
	return old == null; // Error condition
    }
}
