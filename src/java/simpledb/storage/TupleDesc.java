package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    //存储表头的属性内容
    private List<TDItem> tdItems = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     * <p>
     * 属性
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     *
     * 返回列表的迭代器，供遍历表头TupleDesc属性
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     *
     * 根据类型Type、field名建立表头TupleDesc
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == fieldAr.length) {
            for (int i = 0; i < typeAr.length; i++) {
                tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        for (int i = 0; i < typeAr.length; i++) {
            tdItems.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     *
     * 返回表头TupleDesc属性数量
     */
    public int numFields() {
        // some code goes here
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     *
     * 获取表头第i个属性的名字
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.tdItems.size()) {
            throw new NoSuchElementException("位置" + i + "下标无效");
        }
        return this.tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     *
     * 获取表头第i个属性的类型
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.tdItems.size()) {
            throw new NoSuchElementException("位置" + i + "下标无效");
        }
        return this.tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     *
     * 返回属性的在表头TupleDesc中的下标
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException("无法找到该属性");
        }
        for (int i = 0; i < this.tdItems.size(); i++) {
            TDItem tdItem = this.tdItems.get(i);
            if (name.equals(tdItem.fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException("无法找到该属性");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     *
     * 获取表头所有Field的大小：Int 4字节、String 128字节
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < this.tdItems.size(); i++) {
            size += this.tdItems.get(i).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     *
     * 合并两个表头TupleDesc的属性 -> 构建新的表头TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //预处理数据
        Type[] typeArr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldArr = new String[td1.numFields() + td2.numFields()];
        for (int i = 0; i < td1.numFields(); i++) {
            typeArr[i] = td1.tdItems.get(i).fieldType;
            fieldArr[i] = td1.tdItems.get(i).fieldName;
        }
        for (int i = 0; i < td2.numFields(); i++) {
            typeArr[i + td1.numFields()] = td2.tdItems.get(i).fieldType;
            fieldArr[i + td1.numFields()] = td2.tdItems.get(i).fieldName;
        }
        //构建新TupleDesc
        return new TupleDesc(typeArr, fieldArr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     *
     * 根据表头属性数量、属性值来判断是否相等
     */

    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (this.getClass().isInstance(o)) {
            TupleDesc other = (TupleDesc) o;
            if (numFields() == other.numFields()) {
                for (int i = 0; i < numFields(); i++) {
                    if (this.tdItems.get(i).fieldType.equals(other.tdItems.get(i).fieldType)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        return "TupleDesc{" +
                "tdItems=" + tdItems +
                '}';
    }
}
