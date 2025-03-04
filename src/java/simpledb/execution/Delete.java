package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    // 要删除的元组 迭代器
    private OpIterator child;

    // 是否已经删除
    private boolean isDeleted;
    // 返回删除的行数量
    private final TupleDesc tupleDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.isDeleted = false;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"the number of deleted tuple"});
    }

    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();
    }

    @Override
    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.isDeleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // 如果还没有开始删除
        if (!this.isDeleted) {
            this.isDeleted = true;
            int count = 0;
            while (this.child.hasNext()) {
                Tuple tuple = this.child.next();
                try {
                    Database.getBufferPool().deleteTuple(this.tid, tuple);
                    count++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Tuple res = new Tuple(this.tupleDesc);
            res.setField(0, new IntField(count));
            return res;
        }
        return null;
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
    }

}
