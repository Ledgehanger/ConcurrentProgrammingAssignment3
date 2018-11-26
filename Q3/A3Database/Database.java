import java.util.HashMap;

class Database {
    private final HashMap<String,Table> tables;

    Database() {
	tables = new HashMap<String,Table>();
    }

    public void addTable( Table table ) {
	tables.put( table.getName(), table );
    }
    public Table getTable( String name ) {
	return tables.get( name );
    }
}
