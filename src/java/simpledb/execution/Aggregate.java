package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    // 需要聚合的tuples
    private OpIterator child;
    // 待聚合字段的序号
    private int aField;
    // 分组字段的序号
    private int gField;
    // 运算符
    private Aggregator.Op aop;
    // 进行聚合操作的类
    private Aggregator aggregator;
    // 聚合结果的迭代器
    private OpIterator it;
    // 聚合结果的属性行
    private TupleDesc td;


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param aField The column over which we are computing an aggregate.
     * @param gField The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int aField, int gField, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aField = aField;
        this.gField = gField;
        this.aop = aop;

        Type gFieldType = gField == -1 ? null : this.child.getTupleDesc().getFieldType(this.gField);

        if (this.child.getTupleDesc().getFieldType(this.aField) == (Type.STRING_TYPE)) {
            this.aggregator = new StringAggregator(this.gField, gFieldType, this.aField, this.aop);
        } else {
            this.aggregator = new IntegerAggregator(this.gField, gFieldType, this.aField, this.aop);
        }
        this.it = this.aggregator.iterator();
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        if (gFieldType != null) {
            types.add(gFieldType);
            names.add(this.child.getTupleDesc().getFieldName(this.gField));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.aField));
        names.add(this.child.getTupleDesc().getFieldName(this.aField));
        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
    }


    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return this.td.getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        if (this.gField == -1) {
            return this.td.getFieldName(0);
        } else {
            return this.td.getFieldName(1);
        }
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        this.child.open();
        while (this.child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (this.it.hasNext()) {
            return this.it.next();
        }
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    @Override
    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.it.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Type gFieldType = this.gField == -1 ? null : this.child.getTupleDesc().getFieldType(this.gField);
        if (gFieldType != null) {
            types.add(gFieldType);
            names.add(this.child.getTupleDesc().getFieldName(this.gField));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.aField));
        names.add(this.child.getTupleDesc().getFieldName(this.aField));
        if (this.aop.equals(Aggregator.Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
    }
}