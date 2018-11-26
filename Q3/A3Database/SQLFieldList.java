import java.lang.Iterable;
import java.util.Iterator;
import java.util.ArrayList;

class SQLFieldList implements Iterable<SQLField> {
    private ArrayList<SQLField> fields;

    SQLFieldList( int estimated_size ) {
	fields = new ArrayList<SQLField>( estimated_size );
    }

    public boolean containsSummaryField() {
	for( SQLField f : fields ) {
	    if( f.isSummaryField() )
		return true;
	}
	return false;
    }

    public int getSummaryField() {
	for( SQLField f : fields ) {
	    if( f.isSummaryField() )
		return f.getAggregation();
	}
	return ResultSet.AGGREGATE_APPEND;
    }

    public void addField( SQLField f ) {
	fields.add( f );
    }

    public int size() { return fields.size(); }

    public Iterator<SQLField> iterator() {
	return fields.iterator();
    }
}
