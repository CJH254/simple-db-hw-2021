package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    // running SUM,MIN,MAX,COUNT
    private Map<Field, Integer> groupMap;
    private Map<Field, Integer> countMap;
    private Map<Field, List<Integer>> avgMap;


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
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
        this.avgMap = new HashMap<>();
        this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField afield = (IntField) tup.getField(this.afield);
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        int newValue = afield.getValue();
        if (gbfield != null && gbfield.getType() != this.gbfieldtype) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        // get number
        switch (this.what) {
            case MIN:
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, newValue);
                } else {
                    this.groupMap.put(gbfield, Math.min(this.groupMap.get(gbfield), newValue));
                }
                break;

            case MAX:
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, newValue);
                } else {
                    this.groupMap.put(gbfield, Math.max(this.groupMap.get(gbfield), newValue));
                }
                break;

            case SUM:
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, newValue);
                } else {
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield) + newValue);
                }
                break;

            case COUNT:
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, 1);
                } else {
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
                }
                break;

            case SC_AVG:
                IntField countField = null;
                if (gbfield == null) {
                    countField = (IntField) tup.getField(1);
                } else {
                    countField = (IntField) tup.getField(2);
                }
                int countValue = countField.getValue();
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, newValue);
                    this.countMap.put(gbfield, countValue);
                } else {
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield) + newValue);
                    this.countMap.put(gbfield, this.countMap.get(gbfield) + countValue);
                }
            case SUM_COUNT:

            case AVG:
                if (!this.avgMap.containsKey(gbfield)) {
                    List<Integer> l = new ArrayList<>();
                    l.add(newValue);
                    this.avgMap.put(gbfield, l);
                } else {
                    // reference
                    List<Integer> l = this.avgMap.get(gbfield);
                    l.add(newValue);
                }
                break;
            default:
                throw new IllegalArgumentException("Aggregate not supported!");
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
    @Override
    public OpIterator iterator() {
        // some code goes here
        throw new
        UnsupportedOperationException("please implement me for lab2");
    }

}
