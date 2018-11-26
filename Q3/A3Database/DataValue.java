abstract class DataValue implements Comparable<DataValue> {
    // Makes it impossible to create a DataValue.
    protected DataValue() { }

    public abstract void print();

    public abstract int compareTo( DataValue v );
    public abstract DataValue construct( String v );
    public abstract int toInteger();
    public abstract String toString();
}
