package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 * <p>
 * <p>
 * id(int)  name(string)  sex(string)
 * 1           xxx         m
 * 2           yyy         f
 * 那么(1, xxx, m)就是一个Tuple，然后TupleDesc是(id(int) name(string) sex(string))。
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    // 该元组的描述信息
    private TupleDesc tupleDesc;
    // 该元组的id
    private RecordId recordId;
    // 该元组的所有字段
    private List<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.fields = new ArrayList<>(td.numFields());
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     *
     * 设置某字段的值
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i >= this.fields.size()) {
            this.fields.add(i, f);
        } else {
            this.fields.set(i, f);
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        return this.fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        return "Tuple{" +
                "tupleDesc=" + tupleDesc +
                ", fields=" + fields +
                '}';
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     *
     * 返回该元组的具体字段
     */
    public Iterator<Field> fields() {
        // some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * <p>
     * 更换表头TupleDesc
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
    }
}
