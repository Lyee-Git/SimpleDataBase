package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private TupleDesc Td;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final HashMap<Field, Integer> groupMap;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (what != Op.COUNT)
            throw new IllegalArgumentException("Only support COUNT for String Type");
        this.Td = gbfield == NO_GROUPING ?
                new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggrVal"}) :
                new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, new String[] {"groupVal", "aggrVal"});
        groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        StringField aFd = (StringField) tup.getField(afield);
        String aggrVal = aFd.getValue();
        Field gbFd = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        if (gbFd != null && gbFd.getType() != this.gbfieldtype)
            throw new IllegalArgumentException("Wrong group field Type");
        switch (what) {
            case COUNT -> groupMap.put(gbFd, groupMap.getOrDefault(gbFd, 0) + 1);
            default -> throw new IllegalArgumentException("Unimplemented aggregate Operator");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> Tuples= new ArrayList<>();
        for (Field f : groupMap.keySet()) {
            Tuple tuple = new Tuple(Td);
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
        return new TupleIterator(Td, Tuples);
    }

    @Override
    public TupleDesc getTupleDesc() {
        return Td;
    }

}
