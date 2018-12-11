import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.locks.ReentrantReadWriteLock;


class RecordVisitorLock implements RecordVisitor {
    private Table table;
    private LockSet lockSet;
    private ArrayList<Record> records;

    RecordVisitorLock( Table table_, LockSet lockSet_) {
        table = table_;
        lockSet = lockSet_;
        records = new ArrayList<>();
    }

    public ArrayList<Record> getRecords(){
        return records;
    }

    public void visit( Record row ) {
        // The record row needs to be locked.
        ReentrantReadWriteLock.ReadLock readRecordLock = row.getRWLock().readLock();
        lockSet.add(readRecordLock);
        records.add(row);
        readRecordLock.lock();
    }

    public void sortRecords(){
        Cmp recordSort = new Cmp();
        Collections.sort(records, recordSort);
    }

    // This comparator class can be helpful to sort records as part of the
    // deadlock prevention policy.
    class Cmp implements Comparator<Record> {
        // Return a value less than zero if lhs < rhs
        // Return zero if lhs == rhs
        // Return a value greater than zero if lhs > rhs
        // DONE - NEEDS TESTED

        public int compare(Record lhs, Record rhs ) {
            int aid_pos = table.getSchema().getColumnInfo("aid").getPosition();
            if(lhs.get(aid_pos).toInteger() < rhs.get(aid_pos).toInteger())
                return -1;
            else if(lhs.get(aid_pos).toInteger() == rhs.get(aid_pos).toInteger())
                return 0;
            else if(lhs.get(aid_pos).toInteger() > rhs.get(aid_pos).toInteger())
                return 1;
                // This should never happen
            else
                return Integer.MIN_VALUE;
        }
    }

}
