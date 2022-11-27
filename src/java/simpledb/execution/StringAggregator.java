package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;

    private Map<Field, Integer> groupMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException("Only COUNT is supported for String fields!");
        }
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField aField = (StringField) tup.getField(this.aField);
        Field gbField = this.gbField == NO_GROUPING ? null : tup.getField(this.gbField);
        String value = aField.getValue();
        if (gbField != null && gbField.getType() != this.gbFieldType) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        if (!this.groupMap.containsKey(gbField)) {
            this.groupMap.put(gbField, 1);
        } else {
            this.groupMap.put(gbField, this.groupMap.get(gbField) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    @Override
    public OpIterator iterator() {
        return new AggregateIterator(this.groupMap, this.gbFieldType);
    }
}

class AggregateIterator implements OpIterator {

    protected Iterator<Map.Entry<Field, Integer>> it;
    TupleDesc td;

    private Map<Field, Integer> groupMap;
    protected Type itGbFieldType;

    public AggregateIterator(Map<Field, Integer> groupMap, Type gbfieldtype) {
        this.groupMap = groupMap;
        this.itGbFieldType = gbfieldtype;
        // no grouping
        if (this.itGbFieldType == null) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            this.td = new TupleDesc(new Type[]{this.itGbFieldType, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
    }


    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.it = this.groupMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return this.it.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        Map.Entry<Field, Integer> entry = this.it.next();
        Field f = entry.getKey();
        Tuple rtn = new Tuple(this.td);
        this.setFields(rtn, entry.getValue(), f);
        return rtn;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.it = this.groupMap.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void close() {
        this.it = null;
        this.td = null;
    }

    void setFields(Tuple rtn, int value, Field f) {
        if (f == null) {
            rtn.setField(0, new IntField(value));
        } else {
            rtn.setField(0, f);
            rtn.setField(1, new IntField(value));
        }
    }
}


