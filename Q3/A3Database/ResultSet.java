import java.lang.Iterable;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

class ResultSet implements Iterable<Record> {
    private boolean m_success;
    private final List<Record> rows;
    private final int aggregate;
    private int aggval;
    private int nrecords;
    // private TableSchema schema;

    static public final int AGGREGATE_APPEND = 0;
    static public final int AGGREGATE_COUNT = 1;
    static public final int AGGREGATE_SUM = 2;
    static public final int AGGREGATE_UPDATE = 3;

    ResultSet( int agg, boolean success_ ) {
	m_success = success_;
	rows = new LinkedList<Record>();
	aggregate = agg;
	aggval = 0;
	nrecords = 0;
    }
    ResultSet( int agg ) {
	this( agg, false );
    }
    ResultSet( boolean success_ ) {
	this( AGGREGATE_APPEND, success_ );
    }
    ResultSet() {
	this( AGGREGATE_APPEND, false );
    }

    public boolean success() {
	return m_success;
    }
    public void set( boolean success_ ) {
	m_success = success_;
    }

    public int count() {
	return nrecords; // rows.size();
    }

    public Record get( int row ) {
	return rows.get( row );
    }

    public Iterator<Record> iterator() {
	return rows.iterator();
    }

    public void add( Record row ) {
	switch( aggregate ) {
	case AGGREGATE_APPEND:
	    rows.add( row );
	    nrecords++;
	    break;
	case AGGREGATE_SUM:
	    aggval += row.get( 0 ).toInteger();
	    break;
	case AGGREGATE_COUNT:
	    aggval++;
	    break;
	case AGGREGATE_UPDATE:
	    nrecords++;
	    break;
	}
    }

    public boolean isEmpty() {
	return rows.isEmpty();
    }

    public void print() {
	if( m_success ) {
	    int num = 0;
	    for( Record r : rows ) {
		r.print();
		++num;
	    }
	    System.out.println( num + " rows returned.\n" );
	    System.err.println( "Success." );
	} else {
	    System.err.println( "Failed." );
	}
    }

    public void finalise() {
	switch( aggregate ) {
	case AGGREGATE_APPEND:
	case AGGREGATE_UPDATE:
	    break;
	case AGGREGATE_SUM:
	case AGGREGATE_COUNT:
	    Record row = new Record( 1 );
	    row.add( 0, new IntegerDataValue( aggval ) );
	    rows.add( row );
	    nrecords = 1;
	    break;
	}
    }
}
