
import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  // make hierarchy of directories, root directory of key value pairs

  private HashMap<String, TableMetadata> tables;
  private FDB fdb;
  private Database db;
  private DirectorySubspace rootDir;

  // constructor for class
  public TableManagerImpl(){
    tables = new HashMap<>();
    fdb = FDB.selectAPIVersion(710);

    // open database
    try {
      db = fdb.open();
      System.out.println("Database opened!");
    } catch (Exception e) {
      System.out.println("ERROR: the database not successfully opened: " + e);
    }

    // instantiate root directory
    try {
      if (rootDir == null)
      {
        rootDir = DirectoryLayer.getDefault().createOrOpen(db,
                PathUtil.from("Tables")).join();
        System.out.println("Root dir made!");
      }
    } catch (Exception e) {
      System.out.println("ERROR: root dir not made: " + e);
    }
  }

  public static void addAttributeValuePairToTable(Transaction tx, DirectorySubspace table,
                                                  int primaryKey, String attributeName, Object attributeValue) {
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(primaryKey).add(attributeName);

    Tuple valueTuple = new Tuple();
    valueTuple = valueTuple.addObject(attributeValue);
    tx.set(table.pack(keyTuple), valueTuple.pack());
  }

  // primaryKeyAttributeNames is subset of attributeNames
  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {

    // TODO: check parameters before doing anything with database

    // create table
    final DirectorySubspace tableDir = rootDir.createOrOpen(db, PathUtil.from(tableName)).join();

    System.out.println(tableName + " table created successfully!");

    int numPrimaryKeys = primaryKeyAttributeNames.length;

    // Then subdirectories of primaryKeys and attributes

    final DirectorySubspace attributeDir = tableDir.createOrOpen(db, PathUtil.from("attributes")).join();
    final DirectorySubspace primaryKeyDir = tableDir.createOrOpen(db, PathUtil.from("primaryKeys")).join();

    // primary keys first, then followed by rest of attribute names

    // Add primaryKeys to row
    Transaction tx = db.createTransaction();
    Tuple completeKey = new Tuple();
    for (int i = 0; i < numPrimaryKeys; i++)
    {
      Tuple keyTuple = new Tuple();

      keyTuple.add(primaryKeyAttributeNames[i]);
      keyTuple.addObject(attributeType[i]);

      completeKey.add(keyTuple);
    }

    // Next add rest of attributes
    Tuple completeValue = new Tuple();
    for (int j = numPrimaryKeys; j < numPrimaryKeys + attributeNames.length; j++)
    {
      Tuple valueTuple = new Tuple();
      valueTuple.add(attributeNames[j]).add(Tuple.from(attributeType[j]).pack());

      completeValue.add(valueTuple);
    }

    tx.set(completeKey.pack(), completeValue.pack());

    // check for table first
/*    for (String key : tables.keySet())
    {
      if (tableName.equals(key))
        return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }*/

    // check for valid primaryKeys
/*    if (primaryKeyAttributeNames.length > 0)
    {
      for (String key : primaryKeyAttributeNames)
      {
        if (key == "")
          return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
      }
    }*/
/*    else
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;

    // check for valid attributes
    if (attributeNames.length != attributeType.length)
    {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }*/

    // create metadata of new table public TableMetadata(String[] attributeNames, AttributeType[] attributeTypes, String[] primaryKeys)
    TableMetadata tmd = new TableMetadata(attributeNames, attributeType, primaryKeyAttributeNames);
    tables.put(tableName, tmd);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // check if table exists
    if (!tables.containsKey(tableName))
    {
      return StatusCode.TABLE_NOT_FOUND;
    }
    // connect to db to do so
    try(Database db = fdb.open()) {
      Transaction transaction = db.createTransaction();
      Tuple tuple = Tuple.from(tableName);
      transaction.clear(tuple.range());
      transaction.commit();
    }
    catch (Exception e)
    {
      System.out.println(e);
    }
    // from local hashmap
    tables.remove(tableName);
    return StatusCode.SUCCESS;

  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    return tables;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    TableMetadata value = tables.get(tableName);
    // check table
    /*if (value == null)
    {
      return StatusCode.TABLE_NOT_FOUND;
    }
    // check attribute name/type
    if (attributeName == "" || attributeType == null)
    {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    // attribute already exists
    if (value.getAttributes().containsKey(attributeName))
    {
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }

    value.getAttributes().put(attributeName, attributeType);*/

    Tuple keyTuple = new Tuple();
    //keyTuple = keyTuple.add()

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    TableMetadata value = tables.get(tableName);
    // check if table exists
    if (value == null)
    {
      return StatusCode.TABLE_NOT_FOUND;
    }
    // check attribute name/type
    if (attributeName == "")
    {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    // attribute already exists
    HashMap<String, AttributeType> attributes = value.getAttributes();
    if (attributes.containsKey(attributeName))
    {
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }
    else
    {
      // remove from db
      try(Database db = fdb.open()) {
        Transaction transaction = db.createTransaction();
        Tuple tuple = Tuple.from(tableName);
        transaction.clear(tuple.range());
        transaction.commit();
      }
      catch (Exception e)
      {
        System.out.println(e);
      }
      // remove from local
      attributes.remove(attributeName);

      return StatusCode.SUCCESS;
    }
  }

  @Override
  public StatusCode dropAllTables() {
    // clear all inner tables
    for (Map.Entry<String, TableMetadata> entry: tables.entrySet()){
      deleteTable(entry.getKey());
    }

    tables.clear();

    return StatusCode.SUCCESS;
  }
}
