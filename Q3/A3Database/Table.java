import java.util.Map;
import java.util.Iterator;

class Table {
    private final String name;
    private final TableIndex primary_index;
    private final TableSchema schema;
    
    // Constructor
    Table( final String name_, final TableSchema schema_ ) {
	name = name_;
	schema = schema_;
	primary_index = new TableIndex( schema.getKey() );
    }

    // Get the name of the table
    public String getName() {
	return name;
    }

    // Check if a particular field (column) is a key
    public boolean isKey( final String field_name ) {
	return schema.isKey( field_name );
    }

    // Return the schema
    public TableSchema getSchema() { return schema; }

    public void visit( final RecordVisitor method, final SQLWhereClause clause ) {
	if( clause == null ) {
	    // Visit all rows
	    primary_index.visitAll( method );
	} else {
	    String field_name = clause.getFieldName();
	    DataValue value = clause.getValue();
	    if( schema.isKey( field_name ) ) {
		switch( clause.getComparator() ) {
		case SQLWhereClause.CMP_EQ:
		    primary_index.visitOne( value, method );
		    break;
		case SQLWhereClause.CMP_IN:
		    for( DataValue v : ((SetDataValue)value) )
			primary_index.visitOne( v, method );
		    break;
		default:
		    assert false : "error";
		    return; // Return value???
		}
	    } else {
		RecordVisitor selective_method
		    = new RecordVisitorEvaluateWhere( this, method, clause );
		primary_index.visitAll( selective_method );
	    }
	}
    }

    // Check if row matches where clause
    // This method could move to the schema?
    public boolean matches( Record row, SQLWhereClause where ) {
	// In the absence of a where clause, everything matches
	if( where == null )
	    return true;
	
	// Evaluate the where clause if there is one
	String field = where.getFieldName();
	int position = schema.getColumnInfo( field ).getPosition();
	DataValue val = row.get( position );
	DataValue srch = where.getValue();
	switch( where.getComparator() ) {
	case SQLWhereClause.CMP_EQ:
	    return val.compareTo( srch ) == 0;
	case SQLWhereClause.CMP_LT:
	    return val.compareTo( srch ) < 0;
	case SQLWhereClause.CMP_GT:
	    return val.compareTo( srch ) > 0;
	default:
	    return false;
	}
    }
    
    // Select requested fields of a row
    // This method could move to the schema?
    public Record extract( Record row, SQLFieldList field_list ) {
	Record result = new Record( field_list.size() );
	int pos = 0;
	for( SQLField f : field_list ) {
	    int position = schema.getColumnInfo( f.getName() ).getPosition();
	    result.add( pos, row.get( position ) );
	    ++pos;
	}
	return result;
    }

    // Update a field in a row
    // Note: we should not update a key if the Record is stored in the
    //       index (as opposed to being part of a ResultSet)
    public boolean update( Record row, SQLField field, DataValue value ) {
	ColumnInfo info = schema.getColumnInfo( field.getName() );
	return row.set( info.getPosition(), value );
    }

    // Insert a row in the table. We support only one index, which is a primary
    // index. If we had secondary indices, we would need to insert a pointer
    // to the row in these indices as well.
    public boolean insert( Record row ) {
	return primary_index.insert( row, schema );
    }

}
