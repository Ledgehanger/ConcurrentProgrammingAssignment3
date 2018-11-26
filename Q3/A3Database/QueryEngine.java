// This class executes the SQL statements. Each thread in the driver program
// uses its own instance of the QueryEngine, so you can add variables to
// this class that will be private to each thread.
// The database is obviously shared among all threads.
class QueryEngine {
    private final Database database;

    QueryEngine( Database database_ ) {
	database = database_;
    }

    public ResultSet execute( SQLStatement stmt ) {
	// The following is bad practice, just so you know.
	// Better is to place the execute method in the Statement classes
	// themselves, but here we want to keep the execute code separate
	// so that we can easily replace this file to change the behavior
	// of the system.
	if( stmt instanceof SQLSelectStatement ) {
	    return execute( (SQLSelectStatement)stmt );
	} else if( stmt instanceof SQLSelectForUpdateStatement ) {
	    return execute( (SQLSelectForUpdateStatement)stmt );
	} else if( stmt instanceof SQLUpdateStatement ) {
	    return execute( (SQLUpdateStatement)stmt );
	} else if( stmt instanceof SQLInsertIntoStatement ) {
	    return execute( (SQLInsertIntoStatement)stmt );
	} else if( stmt instanceof SQLCommitStatement ) {
	    return execute( (SQLCommitStatement)stmt );
	} else if( stmt instanceof SQLStartTransactionStatement ) {
	    return execute( (SQLStartTransactionStatement)stmt );
	} else {
	    return new ResultSet(); // failure
	}
    }
    public ResultSet execute( SQLSelectStatement stmt ) {
	Table table = database.getTable( stmt.getTable().getTableName() );
	int method = stmt.getFieldList().getSummaryField();

	// Where we collect the result. Method determines how to collect
	// results, e.g., listing all records, or summarising with count or sum.
	ResultSet result = new ResultSet( method, true );

	// Using an index or scanning the full table, depending on whether we
	// are querying a key. We only support searching for particular values
	// (field = X); range queries (field >,< X) and composite
	// conditions (logical and, or, etc) are not supported.
	table.visit( new RecordVisitorSelect( table, result, stmt.getFieldList() ),
		     stmt.getWhereClause() );

	// Finish of computation of aggregates
	result.finalise();

	return result;
    }
    public ResultSet execute( SQLSelectForUpdateStatement stmt ) {
	// Rather than performing a SELECT operation, just note the records
	// touched and lock them.
	Table table = database.getTable( stmt.getTable().getTableName() );

	// ...
	// Visit all rows identified by SELECT and meeting the WHERE clause.
	// The RecordVisitorLock class is incomplete, it needs to be completed
	// as part of the deadlock avoidance policy.
	RecordVisitorLock method = new RecordVisitorLock( table );
	table.visit( method, stmt.getWhereClause() );
	// ...
	return new ResultSet( true );
    }
    public ResultSet execute( SQLUpdateStatement stmt ) {
	Table table = database.getTable( stmt.getTable().getTableName() );
	ResultSet result = new ResultSet( ResultSet.AGGREGATE_UPDATE, true );

	// Use a visitor to apply the update. The visitor updates the ResultSet
	// to count the number of matching rows and to track presence of errors.
	RecordVisitorUpdate method
	    = new RecordVisitorUpdate( table, stmt.getField(), stmt.getValue(),
				       result );
	table.visit( method, stmt.getWhereClause() );

	return result;
    }
    public ResultSet execute( SQLInsertIntoStatement stmt ) {
	Table table = database.getTable( stmt.getTable().getTableName() );
	TableSchema schema = table.getSchema();

	Record record
	    = schema.assembleRecord( stmt.getFields(), stmt.getValues() );
	if( record == null )
	    return new ResultSet(); // failure
	
	boolean success = table.insert( record );
	return new ResultSet( success );
    }
    public ResultSet execute( SQLStartTransactionStatement stmt ) {
	// A new transaction starts. Prepare to collect locks.
	// You can use the LockSet class in LockSet.java to help you
	// ...
	return new ResultSet( true );
    }
    public ResultSet execute( SQLCommitStatement stmt ) {
	// Release all locks held
	// ...
	return new ResultSet( true );
    }
}
