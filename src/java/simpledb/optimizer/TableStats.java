package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s); // statsMap is a static field so first argument could be omitted
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private int ioCostPerPage;
    private int numPages;
    private int numTuples;
    private TupleDesc tupleDesc;
    HashMap<Integer, IntHistogram> intHistograms;
    HashMap<Integer, StringHistogram> strHistograms;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        intHistograms = new HashMap<>();
        strHistograms = new HashMap<>();
        HeapFile dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.numPages = dbFile.numPages();
        this.ioCostPerPage = ioCostPerPage;
        this.tupleDesc = dbFile.getTupleDesc();
        this.numTuples = 0;
        DbFileIterator child = dbFile.iterator(null);
        HashMap<Integer, Integer> Mins = new HashMap<>();
        HashMap<Integer, Integer> Maxes = new HashMap<>();
        try {
            child.open();
            while (child.hasNext()) {
                Tuple t = child.next();
                numTuples++;
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (t.getField(i).getType() == Type.INT_TYPE) {
                        int minVal = Mins.getOrDefault(i, Integer.MAX_VALUE);
                        IntField f = (IntField) t.getField(i);
                        if (f.getValue() < minVal) {
                            Mins.put(i, f.getValue());
                        }
                        int maxVal = Maxes.getOrDefault(i, Integer.MIN_VALUE);
                        if (f.getValue() > maxVal) {
                            Maxes.put(i, f.getValue());
                        }
                    }
                    // For StringHistogram, add through without min/max scan
                    else {
                        strHistograms.putIfAbsent(i, new StringHistogram(NUM_HIST_BINS));
                        StringHistogram strHistogram = strHistograms.get(i);
                        strHistogram.addValue(t.getField(i).toString());
                    }
                }
            }
            for (int i = 0; i < tupleDesc.numFields(); i++) {
                this.intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, Mins.get(i), Maxes.get(i)));
            }
            child.rewind();
            while (child.hasNext()) {
                Tuple t = child.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (t.getField(i).getType() == Type.INT_TYPE) {
                        int val = ((IntField) t.getField(i)).getValue();
                        intHistograms.get(i).addValue(val);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return ioCostPerPage * numTuples * 2.0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (selectivityFactor * numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram intHistogram = intHistograms.get(field);
            if (intHistogram != null) {
                return intHistogram.avgSelectivity();
            }
        }
        else {
            StringHistogram stringHistogram = strHistograms.get(field);
            if (stringHistogram != null) {
                return stringHistogram.avgSelectivity();
            }
        }
        return 0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram intHistogram = intHistograms.get(field);
            if (intHistogram != null) {
                return intHistogram.estimateSelectivity(op, ((IntField) constant).getValue());
            }
        }
        else {
            StringHistogram stringHistogram = strHistograms.get(field);
            if (stringHistogram != null) {
                return stringHistogram.estimateSelectivity(op, constant.toString());
            }
        }
        return 0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return  numTuples;
    }

}
