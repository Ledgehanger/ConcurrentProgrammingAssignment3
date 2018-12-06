import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;
import java.util.Vector;

/*===========================================================
 * Auxiliary class to perform timing measurement and control
 * the experiments.
 *===========================================================*/
class ResultAggregatorQ3 {
    public static final int NUM_WARMUP = 3;
    private int num_rounds;
    private int num_threads;
    private volatile int round;
    private final CyclicBarrier barrier;
    private long sum_rate;
    private long sum_rate_sq;
    private double sum_delay;
    private long last_time;
    private long num_values[][];
    private volatile boolean round_done;

    ResultAggregatorQ3(int nr, int np ) {
	num_rounds = nr;
	num_threads = np;
	round = 0;
	sum_rate = sum_rate_sq = 0;
	sum_delay = 0;
	last_time = System.nanoTime();

	num_values = new long[np][3];
	for( int i=0; i < np; ++i )
	    num_values[i][0] = num_values[i][1] = num_values[i][2] = 0;

	round_done = false;

	// np+1: main thread also joins barrier to synchronise timer
	barrier = new CyclicBarrier( np+1,
				     new Runnable() {
					 public void run() {
					     long now = System.nanoTime();
					     long delay = now - last_time;
					     double f_delay = (double)delay * 1e-9;
					     long nv = 0, nvs = 0, nvc = 0; 
					     for( int i=0; i < num_threads; ++i ) {
						 nv += num_values[i][0];
						 num_values[i][0] = 0;
						 nvs += num_values[i][1];
						 num_values[i][1] = 0;
						 nvc += num_values[i][2];
						 num_values[i][2] = 0;
					     }

					     round++;
					     if( round > NUM_WARMUP ) { // skip warmup rounds
						 double r = (double)nv / f_delay;
						 double rs = (double)nvs / f_delay;
						 double rc = (double)nvc / f_delay;
						 sum_rate += r;
						 sum_rate_sq += r*r;
						 sum_delay += f_delay;

						 System.out.println( "Delay is " + f_delay + " secs; throughput is " + String.format("%.3f", r ) + "ops/sec of which " + String.format("%.3f", rs ) + " sum and " + String.format("%.3f", rc ) + " create" );
					     } else {
						 System.out.println( "delay is " + f_delay + " secs (warmup)" );
					     }
					     round_done = false;
					     System.gc();
					     System.gc();
					     System.gc();
					     last_time = System.nanoTime();
					 }
				     } );
    }

    public boolean isFinished() {
	return round >= num_rounds + NUM_WARMUP;
    }

    public boolean isRoundDone() {
	return round_done;
    }

    public void setRoundDone() {
	round_done = true;
    }

    public double getAvgDelay() {
	return (double)sum_delay / (double)(num_rounds);
    }

    public double getAvgRate() {
	return (double)sum_rate / (double)(num_rounds);
    }

    public double getStdDevRate() {
	double s1 = (double)sum_rate;
	double s2 = (double)sum_rate_sq;
	double N = (double)(num_rounds);
	return Math.sqrt( (N*s2-s1*s1)/(N*(N-1)) );
    }

    public void syncBarrier( int tid, long nv, long nvs, long nvc ) {
	if( tid >= 0 ) {
	    num_values[tid][0] = nv;
	    num_values[tid][1] = nvs;
	    num_values[tid][2] = nvc;
	}
	try {
	    barrier.await();
	} catch( InterruptedException ex ) {
	} catch( BrokenBarrierException ex ) {
	}
    }
}

/*===========================================================
 * Process definition for processes that will execute queries.
 *===========================================================*/
class TestProcessQ3 extends Thread {
    private final Database database;
    private final Table tbl_account;
    private final AtomicInteger next_account;
    private final Vector<Integer> accounts;
    private final boolean sum_needs_lock;
    private final int tid;
    private final int total;
    private final int freq_sum;
    private final int freq_create;
    private final ResultAggregatorQ3 agg;

    private final IntegerDataValue qv_account0, qv_account1;
    private final SQLStatement qlocka, qsum, qsumlock, qbegintx, qcommit,
	qaccount;
    private final SQLWhereClause where0, where1;
    private final SQLTable t_account;
    private final SQLFieldList f_account;

    TestProcessQ3(Database database_, Table tbl_a,
				  AtomicInteger nxt_a, Vector<Integer> acct,
				  boolean snl, int total_,
				  int freq_sum_, int freq_create_,
				  int tid_, ResultAggregatorQ3 agg_ ) {
	database = database_;
	tbl_account = tbl_a;
	next_account = nxt_a;
	accounts = acct;
	sum_needs_lock = snl;
	total = total_;
	freq_sum = freq_sum_;
	freq_create = freq_create_;
	tid = tid_;
	agg = agg_;

	// Create table names
	t_account = new SQLTable( new String( "account" ) );
	
	// Create queries and variables
	qv_account0 = new IntegerDataValue( 0 );
	qv_account1 = new IntegerDataValue( 0 );
	f_account = new SQLFieldList( 2 );
	f_account.addField( new SQLField( new String( "aid" ) ) );
	f_account.addField( new SQLField( new String( "amount" ) ) );

	final SetDataValue qv_accounts
	    = new SetDataValue( qv_account0, qv_account1 );

	{
	    // START TRANSACTION;
	    qbegintx = new SQLStartTransactionStatement();

	    // COMMIT;
	    qcommit = new SQLCommitStatement();
	}

	// Sum all accounts, lock all accounts
	{
	    // SELECT SUM(amount) FROM account;
	    SQLFieldList fields = new SQLFieldList( 1 );
	    fields.addField( new SQLField( new String( "amount" ),
					   ResultSet.AGGREGATE_SUM ) );

	    qsum = new SQLSelectStatement( fields, t_account );
	    qsumlock = new SQLSelectForUpdateStatement( fields, t_account );
	}

	// Lock accounts
	{
	    // SELECT (aid,amount) FROM basket WHERE aid IN (a0,a1) FOR UPDATE;
	    SQLWhereClause where
		= new SQLWhereClause( new String( "aid" ), // field name
				      qv_accounts, // value (set of 2)
				      SQLWhereClause.CMP_IN );
	    qlocka
		= new SQLSelectForUpdateStatement( f_account, t_account, where );
	}

	// Query accounts
	{
	    // SELECT (aid,amount) FROM basket WHERE aid = aid
	    SQLFieldList fields = new SQLFieldList( 2 );
	    fields.addField( new SQLField( new String( "aid" ) ) );
	    fields.addField( new SQLField( new String( "amount" ) ) );

	    SQLWhereClause where
		= new SQLWhereClause( new String( "aid" ), // field name
				      qv_accounts, // value (set of 2)
				      SQLWhereClause.CMP_IN );

	    qaccount
		= new SQLSelectStatement( fields, t_account, where );
	}

	{
	    // UPDATE account SET amount = %d WHERE aid = %d;
	    where0
		= new SQLWhereClause( new String( "aid" ), // field name
				      qv_account0, // account number
				      SQLWhereClause.CMP_EQ );
	    where1
		= new SQLWhereClause( new String( "aid" ), // field name
				      qv_account1, // account number
				      SQLWhereClause.CMP_EQ );
	}
    }

    private SQLStatement buildQueryUpdate0( int amount0 ) {
	SQLField field = new SQLField( new String( "amount" ) );

	IntegerDataValue qv_amount0 = new IntegerDataValue( amount0 );

	return new SQLUpdateStatement( field, qv_amount0, t_account, where0 );
    }

    private SQLStatement buildQueryUpdate1( int amount1 ) {
	SQLField field = new SQLField( new String( "amount" ) );

	IntegerDataValue qv_amount1 = new IntegerDataValue( amount1 );

	return new SQLUpdateStatement( field, qv_amount1, t_account, where1 );
    }

    private SQLStatement buildQueryInsert( int account ) {
	// INSERT INTO account (aid,amount) VALUES (?,?)
	final SQLValueList v_account = new SQLValueList( 2 );
	v_account.addValue( new IntegerDataValue( account ) );
	v_account.addValue( new IntegerDataValue( 0 ) );

	return new SQLInsertIntoStatement( t_account, f_account, v_account );
    }

    public void run() {
	final Random rng = new Random();
	int idx = 0;
	int vseq = 0;

	QueryEngine executor = new QueryEngine( database );

	try {
	    while( !agg.isFinished() ) {
		long v = 0, vs = 0, vc = 0;
		while( !agg.isRoundDone() ) {
		    if( freq_sum != 0 && ( v * 1023 ) % freq_sum == tid ) {
			// Verify sum of accounts

			// Setup transaction
			executor.execute( qbegintx );

			// Acquire locks
			if( sum_needs_lock )
			    executor.execute( qsumlock );
			
			// Execute the query
			ResultSet result = executor.execute( qsum );

			// Expect one result
			if( !result.success() )
			    throw new Exception( "Query sum accounts failed." );
			if( result.count() != 1 )
			    throw new Exception( "Query sum accounts returns "
						 + result.count() + " results" );

			Record row = result.iterator().next();
			int sum = row.get( 0 ).toInteger();

			executor.execute( qcommit );

			// Check that the correct value was calculated
			if( sum != total )
			    throw new Exception( "Query sum accounts returned "
						 + sum + " but expected " + total );
			++vs;
		    } else if( freq_create != 0 && ( v * 127 ) % freq_create == 1 ) {
			// Create new account

			// Begin TX
			executor.execute( qbegintx );

			// Create and execute the query
			int aid = next_account.getAndIncrement();
			SQLStatement qcreate = buildQueryInsert( aid );
			ResultSet result = executor.execute( qcreate );

			// Let everyone know we have a new account to use
			// Because of potential race conditions, we store all
			// created accounts in the Vector accounts. One would
			// assume it contains all integers in the range 0 to
			// next_account (not inclusive), however, due to
			// concurrency the accounts are not created in strictly
			// increasing order and there may temporarily "holes"
			// in the range of accounts that are still being
			// created. These holes are non-existing accounts and
			// transfer queries against such accounts should fail.
			// Alternatively, if the database was to implement
			// an auto-incrementing primary key, it would tell us
			// the next account number and we could simply store
			// that number in a shared variable.
			if( result.success() )
			    accounts.add( new Integer( aid ) );
			else
			    System.err.println( "Insert query failed on " + aid );

			// Commit TX
			executor.execute( qcommit );

			++vc;
		    } else { // ATM
			// Transfer an amount between two accounts
			
			// Query two accounts, make sure they are different
			int na = accounts.size();
			int seq0 = rng.nextInt( na );
			int seq1 = rng.nextInt( na-1 );
			if( seq1 >= seq0 )
			    seq1 += 1;
			int account0 = accounts.get( seq0 );
			int account1 = accounts.get( seq1 );

			// Begin TX
			executor.execute( qbegintx );
			    
			// Lock accounts
			qv_account0.set( account0 );
			qv_account1.set( account1 );
			executor.execute( qlocka );

			// Read accounts
			ResultSet result = executor.execute( qaccount );

			// Expect two results
			if( !result.success() )
			    throw new Exception( "Query accounts " + account0
						 + " and " + account1
						 + " failed." );
			if( result.count() != 2 )
			    throw new Exception( "Query accounts " + account0
						 + " and " + account1 + " returns "
						 + result.count() + " results" );

			// Extract current amounts held in accounts
			int amount0 = -1, amount1 = -1;
			for( Record row : result ) {
			    if( row.get( 0 ).toInteger() == account0 )
				amount0 = row.get( 1 ).toInteger();
			    else if( row.get( 0 ).toInteger() == account1 )
				amount1 = row.get( 1 ).toInteger();
			    else
				throw new Exception( "Expect values for accounts "
						     + account0 + " and "
						     + account1 + ", got "
						     + row.get( 0 ) );
			}

			// Decide on amount to transfer
			int transfer = amount0 / 2;

			// Perform the transfer
			SQLStatement qupdate;
			qupdate = buildQueryUpdate0( amount0 - transfer );
			ResultSet tfresult = executor.execute( qupdate );

			// Check success
			if( !tfresult.success() || tfresult.count() != 1 )
			    throw new Exception( "Transfer of " + transfer
						 + " from " + account0
						 + " to " + account1
						 + " failed in update 0." );

			qupdate = buildQueryUpdate1( amount1 + transfer );
			tfresult = executor.execute( qupdate );

			// Check success
			if( !tfresult.success() || tfresult.count() != 1 )
			    throw new Exception( "Transfer of " + transfer
						 + " from " + account0
						 + " to " + account1
						 + " failed in update 1." );

			// System.err.println( account0 + " (" + amount0
					    // + ") -> " + account1 + " ("
					    // + amount1 + ") amt: " + transfer );
			// Commit TX
			executor.execute( qcommit );
		    }

		    v++;
		}
		agg.syncBarrier( tid, v, vs, vc );
	    }
	} catch( Exception e ) {
	    System.err.println( "Thread " + tid + ": " + e );
	    System.err.println( "Stack trace:" );
	    for( StackTraceElement ste : e.getStackTrace() ) {
		System.err.println(ste);
	    }
	}
    }
}

class Bank {
    static private Table createTableAccount( Database database ) {
	// Setup the schema for a table. This table has two columns:
	// Column 0: uid of type int, which is a primary key.
	// Column 1: amount of type int.
	TableSchema schema = new TableSchema();
	schema.addColumn( 0, true, ColumnInfo.IntegerType, "aid" );
	schema.addColumn( 1, false, ColumnInfo.IntegerType, "amount" );

	// Create an empty table with this schema
	Table table = new Table( new String( "account" ), schema );

	// Add the table to the database
	database.addTable( table );

	return table;
    }
    static private int populateTableAccount( Table table, int n ) {
	System.err.println( "Populating table ACCOUNT with " + n + " records" );
	Random rnd = new Random();
	int sum = 0;
	for( int aid=0; aid < n; ++aid ) {
	    int amount = 10 + rnd.nextInt( 100 );
	    sum += amount;

	    Record row = new Record( 2 );
	    row.add( 0, new IntegerDataValue( aid ) );
	    row.add( 1, new IntegerDataValue( amount ) );
	    table.insert( row );

	    // System.out.println( "populate: " + aid + ": " + amount );
	}

	return sum;
    }
    public static int parse_integer( String arg ) {
	int i = 0;
	try {
	    i = Integer.parseInt( arg );
	} catch( NumberFormatException e ) {
	    System.err.println( "Argument '" + arg + "' must be an integer" );
	}
	return i;
    }

    public static void main( String[] args ) {
	int num_accounts = 100000;
	int num_threads = 1;
	int num_rounds = 10;
	int freq_sum = 100000;
	int freq_create = 10000;
	int msecs_measured = 1000;
	boolean sum_needs_lock = true;

	{
	    boolean errors = false;
	    
	    int i=0;
	    while( i < args.length ) {
		if( args[i].equals( "--nthreads" ) ) {
		    num_threads = parse_integer( args[i+1] );
		    i += 2;
		} else if( args[i].equals( "--naccounts" ) ) {
		    num_accounts = parse_integer( args[i+1] );
		    i += 2;
		} else if( args[i].equals( "--nrounds" ) ) {
		    num_rounds = parse_integer( args[i+1] );
		    i += 2;
		} else if( args[i].equals( "--fsum" ) ) {
		    freq_sum = parse_integer( args[i+1] );
		    i += 2;
		} else if( args[i].equals( "--fcreate" ) ) {
		    freq_create = parse_integer( args[i+1] );
		    i += 2;
		} else if( args[i].equals( "--msecs" ) ) {
		    msecs_measured = parse_integer( args[i+1] );
		    i += 2;
		} else if( args[i].equals( "--sum_needs_lock" ) ) {
		    if( args[i+1].equalsIgnoreCase( "true" ) )
			sum_needs_lock = true;
		    else if( args[i+1].equalsIgnoreCase( "false" ) )
			sum_needs_lock = false;
		    else {
			System.err.println( "Unknown value '" + args[i+1]
					    + "' for option --sum_needs_lock" );
			errors = true;
		    }
		    i += 2;
		} else {
		    System.err.println( "Unknown command line argument '"
					+ args[i] + "'" );
		    ++i;
		    errors = true;
		}
	    }

	    if( errors )
		return;
	}

	// Report configuration
	System.out.println( "Measuring performance with " + num_threads
			    + " threads." + " Doing " + num_rounds
			    + " rounds of the experiment after "
			    + ResultAggregatorQ3.NUM_WARMUP + " warmup"
			    + " round(s) during " + msecs_measured
			    + " milliseconds per round."
			    + " Running SUM query once every " + freq_sum
			    + " queries."
			    );

	// Create a database
	Database database = new Database();

	// Setup the tables
	Table tbl_account = createTableAccount( database );

	// Populate the tables
	int sum_accounts = populateTableAccount( tbl_account, num_accounts );

	ResultAggregatorQ3 agg = new ResultAggregatorQ3( num_rounds, num_threads );

	TestProcessQ3[] processes = new TestProcessQ3[num_threads];

	AtomicInteger ai_accounts = new AtomicInteger( num_accounts );
	Vector<Integer> accounts = new Vector<Integer>( num_accounts, 1024 );
	for( int i=0; i < num_accounts; ++i )
	    accounts.add( new Integer(i) );

	// Create all of the threads
	for( int i=0; i < num_threads; ++i ) {
	    processes[i]
		= new TestProcessQ3( database, tbl_account,
				   ai_accounts, accounts, sum_needs_lock,
				   sum_accounts, freq_sum,
				   freq_create, i, agg );
	}

	// Start all of the threads and let them warmup.
	// Warming up is important to let the JIT do it's work (i.e.,
	// compile and optimize the code to make ti faster). When the JIT
	// kicks in, measured performance numbers are distorted and unreliable.
	// The garbage collector is another source of disruption to performance.
	// That's why will call the GC explicitly when reaching the barrier
	// (see barrier creation). You would normally never call the GC
	// directly.
	for( TestProcessQ3 p : processes )
	    p.start();

	// Trigger timer to terminate every round
	for(int r = 0; r < num_rounds + ResultAggregatorQ3.NUM_WARMUP; ++r ) {
	    try {
		Thread.sleep( msecs_measured );
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	    agg.setRoundDone();
	    agg.syncBarrier( -1, 0, 0, 0 );
	}

	// Join threads (cleanup properly).
	for( TestProcessQ3 p : processes )
	    try { p.join(); } catch( InterruptedException e ) { }

	// Get the results out.
	double avg_delay = agg.getAvgDelay();
	double avg_rate = agg.getAvgRate();
	double sdv_rate = agg.getStdDevRate();

	System.out.println( "Average delay of a trial was " + avg_delay
			    + " seconds." );
	System.out.println( "Average operations/sec: " + avg_rate );
	System.out.println( "Standard deviation operations/sec: " + sdv_rate );
    }
}
