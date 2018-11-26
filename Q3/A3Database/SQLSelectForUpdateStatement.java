class SQLSelectForUpdateStatement extends SQLSelectBaseStatement {
    SQLSelectForUpdateStatement( SQLFieldList field_list_,
				 SQLTable table_,
				 SQLWhereClause where_clause_
				     ) {
	super( field_list_, table_, where_clause_ );
    }
    SQLSelectForUpdateStatement( SQLFieldList field_list_,
				 SQLTable table_ ) {
	super( field_list_, table_, null );
    }
}
