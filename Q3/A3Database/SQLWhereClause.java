class SQLWhereClause {
    // Types of conditionals/comparisons supported
    public static final int CMP_EQ = 1;
    public static final int CMP_LT = 2;
    public static final int CMP_GT = 3;
    public static final int CMP_IN = 4; // set membership
    
    private final String field_name;
    private final DataValue value;
    private final int comparator;

    SQLWhereClause( String field_name_, DataValue value_, int comparator_ ) {
	field_name = field_name_;
	value = value_;
	comparator = comparator_;
    }

    public String getFieldName() { return field_name; }
    public DataValue getValue() { return value; }
    public int getComparator() { return comparator; }
};
