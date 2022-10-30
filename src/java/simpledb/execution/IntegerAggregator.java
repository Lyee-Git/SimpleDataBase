package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final TupleDesc Td;
    private final HashMap<Field, Integer> groupMap;
    private final HashMap<Field, ArrayList<Integer>> avgMap;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.Td = gbfield == NO_GROUPING ?
                new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggrVal"}) :
                new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, new String[] {"groupVal", "aggrVal"});
        this.groupMap = new HashMap<>();
        this.avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField aFd = (IntField) tup.getField(afield);
        int aggrVal = aFd.getValue();
        IntField gbFd = gbfield == NO_GROUPING ? null : (IntField) tup.getField(gbfield);
        // check for group field type
        if (gbFd != null && gbFd.getType() != this.gbfieldtype)
            throw new IllegalArgumentException("Wrong group field Type");
        switch (what) {
            case MAX -> groupMap.put(gbFd, Math.max(groupMap.getOrDefault(gbFd, aggrVal), aggrVal)); // default:aggrVal
            case MIN -> groupMap.put(gbFd, Math.min(groupMap.getOrDefault(gbFd, aggrVal), aggrVal));
            case SUM -> groupMap.put(gbFd, groupMap.getOrDefault(gbFd, 0) + aggrVal);
            case COUNT -> groupMap.put(gbFd, groupMap.getOrDefault(gbFd, 0) + 1);
            case AVG -> avgMap.getOrDefault(gbFd, new ArrayList<Integer>()).add(aggrVal);
            default -> throw new IllegalArgumentException("Unimplemented aggregate Operator");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> Tuples = new ArrayList<>();
        if (what == Op.AVG) {
            Tuple tuple = new Tuple(Td);
            for (Field f : groupMap.keySet()) {
                ArrayList<Integer> valList = avgMap.get(f);
                int sum = 0;
                for (int val : valList)
                    sum += val;
                if (gbfield == NO_GROUPING) {
                    tuple.setField(0, new IntField(sum / valList.size()));
                }
                else {
                    tuple.setField(0, f);
                    tuple.setField(1, new IntField(sum / valList.size()));
                }
                Tuples.add(tuple);
            }
        }
        else {
            Tuple tuple = new Tuple(Td);
            for (Field f : groupMap.keySet()) {
                int val = groupMap.get(f);
                if (gbfield == NO_GROUPING) {
                    tuple.setField(0, new IntField(val));
                }
                else {
                    tuple.setField(0, f);
                    tuple.setField(1, new IntField(val));
                }
                Tuples.add(tuple);
            }
        }
        return new TupleIterator(Td, Tuples);
    }

    @Override
    public TupleDesc getTupleDesc() {
        return Td;
    }

}
