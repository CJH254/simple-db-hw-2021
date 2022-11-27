package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    // 分组字段的序号
    private int gbFieldIndex;
    // 分组字段的类型
    private Type gbFieldType;
    // 聚合字段的序号
    private int aggFieldIndex;
    // 聚合操作
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
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFieldIndex = afield;
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
        // 获取聚合字段
        IntField aggField = (IntField) tup.getField(this.aggFieldIndex);
        // 获取聚合字段的值
        int value = aggField.getValue();
        // 获取分组字段，如果仅是聚合，则该字段为null
        Field gbField = this.gbFieldIndex == NO_GROUPING ? null : tup.getField(this.gbFieldIndex);
        if (gbField != null && gbField.getType() != this.gbFieldType) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        // 根据聚合运算符处理数据逻辑
        switch (this.what) {
            case MIN:
                if (!this.groupMap.containsKey(gbField)) {
                    this.groupMap.put(gbField, value);
                } else {
                    this.groupMap.put(gbField, Math.min(this.groupMap.get(gbField), value));
                }
                break;

            case MAX:
                if (!this.groupMap.containsKey(gbField)) {
                    this.groupMap.put(gbField, value);
                } else {
                    this.groupMap.put(gbField, Math.max(this.groupMap.get(gbField), value));
                }
                break;

            case SUM:
                if (!this.groupMap.containsKey(gbField)) {
                    this.groupMap.put(gbField, value);
                } else {
                    this.groupMap.put(gbField, this.groupMap.get(gbField) + value);
                }
                break;

            case COUNT:
                if (!this.groupMap.containsKey(gbField)) {
                    this.groupMap.put(gbField, 1);
                } else {
                    this.groupMap.put(gbField, this.groupMap.get(gbField) + 1);
                }
                break;

            case SC_AVG:
                IntField countField = null;
                if (gbField == null) {
                    countField = (IntField) tup.getField(1);
                } else {
                    countField = (IntField) tup.getField(2);
                }
                int countValue = countField.getValue();
                if (!this.groupMap.containsKey(gbField)) {
                    this.groupMap.put(gbField, value);
                    this.countMap.put(gbField, countValue);
                } else {
                    this.groupMap.put(gbField, this.groupMap.get(gbField) + value);
                    this.countMap.put(gbField, this.countMap.get(gbField) + countValue);
                }
            case SUM_COUNT:

            case AVG:
                if (!this.avgMap.containsKey(gbField)) {
                    List<Integer> l = new ArrayList<>();
                    l.add(value);
                    this.avgMap.put(gbField, l);
                } else {
                    // reference
                    List<Integer> l = this.avgMap.get(gbField);
                    l.add(value);
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
        return new IntAggIterator();
    }

    private class IntAggIterator extends AggregateIterator {

        private Iterator<Map.Entry<Field, List<Integer>>> avgIt;
        private boolean isAvg;
        private boolean isSCAvg;
        private boolean isSumCount;

        IntAggIterator() {
            super(IntegerAggregator.this.groupMap, IntegerAggregator.this.gbFieldType);
            this.isAvg = IntegerAggregator.this.what.equals(Op.AVG);
            this.isSCAvg = IntegerAggregator.this.what.equals(Op.SC_AVG);
            this.isSumCount = IntegerAggregator.this.what.equals(Op.SUM_COUNT);
            if (this.isSumCount) {
                this.td = new TupleDesc(new Type[]{this.itGbFieldType, Type.INT_TYPE, Type.INT_TYPE},
                        new String[]{"groupVal", "sumVal", "countVal"});
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            if (this.isAvg || this.isSumCount) {
                this.avgIt = IntegerAggregator.this.avgMap.entrySet().iterator();
            } else {
                this.avgIt = null;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.isAvg || this.isSumCount) {
                return this.avgIt.hasNext();
            }
            return super.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple rtn = new Tuple(this.td);
            if (this.isAvg || this.isSumCount) {
                Map.Entry<Field, List<Integer>> avgOrSumCountEntry = this.avgIt.next();
                Field avgOrSumCountField = avgOrSumCountEntry.getKey();
                List<Integer> avgOrSumCountList = avgOrSumCountEntry.getValue();
                if (this.isAvg) {
                    int value = this.sumList(avgOrSumCountList) / avgOrSumCountList.size();
                    this.setFields(rtn, value, avgOrSumCountField);
                    return rtn;
                } else {
                    this.setFields(rtn, sumList(avgOrSumCountList), avgOrSumCountField);
                    if (avgOrSumCountField != null) {
                        rtn.setField(2, new IntField(avgOrSumCountList.size()));
                    } else {
                        rtn.setField(1, new IntField(avgOrSumCountList.size()));
                    }
                    return rtn;
                }
            } else if (this.isSCAvg) {
                Map.Entry<Field, Integer> entry = this.it.next();
                Field f = entry.getKey();
                this.setFields(rtn, entry.getValue() / IntegerAggregator.this.countMap.get(f), f);
                return rtn;
            }
            return super.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            if (this.isAvg || this.isSumCount) {
                this.avgIt = IntegerAggregator.this.avgMap.entrySet().iterator();
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            return super.getTupleDesc();
        }

        @Override
        public void close() {
            super.close();
            this.avgIt = null;
        }

        private int sumList(List<Integer> l) {
            int sum = 0;
            for (int i : l) {
                sum += i;
            }
            return sum;
        }
    }
}
