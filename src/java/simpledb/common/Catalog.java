package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
//Catalog「数据库目录」 -> Table「数据表」= DbFile -> heapPage -> Tuple -> TupleDesc
public class Catalog {

    //MVCC后续
    private final ConcurrentHashMap<Integer, Table> hashTable;

    private static class Table {
        private static final long serialVersionUID = 1L;

        private final DbFile dbFile;
        private final String tableName;
        private final String pkField;

        public Table(DbFile file, String name, String pkeyField) {
            dbFile = file;
            tableName = name;
            pkField = pkeyField;
        }

        public DbFile getDbFile() {
            return dbFile;
        }

        public String getTableName() {
            return tableName;
        }

        public String getPkField() {
            return pkField;
        }

        @Override
        public String toString() {
            return tableName + "(" + dbFile.getId() + ":" + pkField + ")";
        }
    }


    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        this.hashTable = new ConcurrentHashMap<Integer, Table>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        hashTable.put(file.getId(), new Table(file, name, pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        Integer id = hashTable.searchValues(1, value -> {
            if (value.getTableName().equals(name)) {
                return value.getDbFile().getId();
            }
            return null;
        });
        if (id != null) {
            return id.intValue();
        } else {
            throw new NoSuchElementException("该表不存在");
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     *
     * 获取该Table的表头信息
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        Table table = hashTable.get(tableid);
        if (table != null) {
            return table.getDbFile().getTupleDesc();
        }
        throw new NoSuchElementException("table id： " + tableid + "无法找到TupleDesc");
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Table table = hashTable.get(tableid);
        if (table != null) {
            return table.getDbFile();
        }
        throw new NoSuchElementException("table id： " + tableid + "无法找到DbFile");
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        Table table = hashTable.get(tableid);
        if (table != null) {
            return table.getPkField();
        }
        throw new NoSuchElementException("table id： " + tableid + "无法找到Primary key");
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return hashTable.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        Table table = hashTable.get(id);
        if (table != null) {
            return table.getTableName();
        }
        throw new NoSuchElementException("table id： " + id + "无法找到table name");
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // some code goes here
        hashTable.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int")) {
                        types.add(Type.INT_TYPE);
                    } else if (els2[1].trim().equalsIgnoreCase("string")) {
                        types.add(Type.STRING_TYPE);
                    } else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) {
                            primaryKey = els2[0].trim();
                        } else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

