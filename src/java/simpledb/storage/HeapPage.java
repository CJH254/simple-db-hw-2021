package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    // HeapPageId来定位该Page属于哪张表，在表中的序号
    final HeapPageId pid;
    // 每个数据页存储的都是同一张表，即表头信息都是一样的
    final TupleDesc td;
    // 数据页头部，使用bitmap保存各个slot的占用情况
    final byte[] header;
    // 具体的记录:数据
    final Tuple[] tuples;
    // slot存储记录:即该数据页Page存储的记录数
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        this.header = new byte[getHeaderSize()];
        for (int i = 0; i < this.header.length; i++) {
            byte b = dis.readByte();
            this.header[i] = b;
        }

        this.tuples = new Tuple[this.numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < this.tuples.length; i++) {
                this.tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     *
     * 一页Page能够存储的元组Tuple和头部Header「该元组使用情况」的数量
     */
    private int getNumTuples() {
        // some code goes here
        //_tuples per page_ = floor((_page size_ * 8) / (_tuple size_ * 8 + 1))
        int num = (int) Math.floor((BufferPool.getPageSize() * 8 * 1.0) / (this.td.getSize() * 8 + 1));
        return num;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * Header Size必须向上取整，给多出来的数据分配完整的内存空间
     */
    private int getHeaderSize() {
        // some code goes here
        // headerBytes = ceiling(tupsPerPage/8)
        return (int) Math.ceil(getNumTuples() * 1.0 / 8);
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    @Override
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (this.oldDataLock) {
                oldDataRef = this.oldData;
            }
            return new HeapPage(this.pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    @Override
    public void setBeforeImage() {
        synchronized (this.oldDataLock) {
            this.oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    @Override
    public HeapPageId getId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < this.td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(this.td);
        RecordId rid = new RecordId(this.pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < this.td.numFields(); j++) {
                Field f = this.td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    @Override
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : this.header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < this.tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < this.td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < this.td.numFields(); j++) {
                Field f = this.tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (this.header.length + this.td.getSize() * this.tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    @Override
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return null;
    }

    /**
     * Returns the number of empty slots on this page.
     * <p>
     * header数组中没使用的情况
     */
    public int getNumEmptySlots() {
        // some code goes here
        int cnt = 0;
        for (int i = 0; i < this.numSlots; ++i) {
            if (!isSlotUsed(i)) {
                ++cnt;
            }
        }
        return cnt;
    }

    /**
     * Returns true if associated slot on this page is filled.
     * <p>
     * header数组的一个位表示一个元组的使用情况
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // use bitmap
        if (i > numSlots) {
            return false;
        }
        //index即指第i页Page
        int index = i / 8;
        int offset = i % 8;
        //每一位就代表一个page，每一个page有8位，所以需要除以8位。
        return (this.header[index] & (1 << offset)) != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     *
     * 返回迭代器，里面的元素都是该页使用的Tuple集合
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        List<Tuple> filledTuples = new ArrayList<Tuple>();
        for (int i = 0; i < this.numSlots; ++i) {
            if (isSlotUsed(i)) {
                filledTuples.add(this.tuples[i]);
            }
        }
        return filledTuples.iterator();
    }

}

