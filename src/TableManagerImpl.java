
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import jdk.net.SocketFlow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
  private DirectorySubspace meta;

  private DirectorySubspace raw;

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
    System.out.println("Running createTable test");

    if (attributeNames == null || attributeType == null)
    {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    if (primaryKeyAttributeNames == null)
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;

    // check for valid primaryKeys and check if they're in attributeNames
  if (primaryKeyAttributeNames.length > 0)
    {
      for (String key : primaryKeyAttributeNames)
      {
        if (key == "")
          return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;

        boolean found = false;
        for (String name : attributeNames){
          if (name.equals(key)){
            found = true;
            break;
          }
        }
        if (!found)
          return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
      }

    }
    else
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;

    // check for valid attributes
    if (attributeNames.length != attributeType.length)
    {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    // create table
    final DirectorySubspace tableDir = rootDir.createOrOpen(db, PathUtil.from(tableName)).join();

    // make meta data and raw data
    meta = tableDir.createOrOpen(db, PathUtil.from("meta")).join();
    raw = tableDir.createOrOpen(db, PathUtil.from("raw")).join();

    Transaction tx = db.createTransaction();
    //completeKey = completeKey.add(0).add(primaryKeyAttributeNames[0]);
    for (int i = 0; i < attributeNames.length; i++)
    {
      Tuple keyTuple = new Tuple();
      keyTuple = keyTuple.add(attributeNames[i]);
      keyTuple = keyTuple.add(attributeType[i].name());

      // primaryKey or not
      boolean found = false;
      for (String s : primaryKeyAttributeNames)
      {
        if (s.equals(attributeNames[i])) {
          found = true;
          break;
        }
      }
      Tuple valueTuple = new Tuple();
      valueTuple = valueTuple.add(found);
      tx.set(meta.pack(keyTuple), valueTuple.pack());
    }

    //meta = Subspace()
    System.out.println(tableName + " table created successfully!");
    System.out.println("createTable Test done");

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
//    // check if table exists
      System.out.println("Running deleteTable");
//    if (!tables.containsKey(tableName))
//    {
//      return StatusCode.TABLE_NOT_FOUND;
//    }
//    // connect to db to do so
//    try(Database db = fdb.open()) {
//      Transaction transaction = db.createTransaction();
//      Tuple tuple = Tuple.from(tableName);
//      transaction.clear(tuple.range());
//      transaction.commit();
//    }
//    catch (Exception e)
//    {
//      System.out.println(e);
//    }
//    // from local hashmap
//    tables.remove(tableName);
    System.out.println("Done with deleteTable");
    return StatusCode.SUCCESS;

  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // initialize HashMap to return
    Map<String, TableMetadata> result = new HashMap<>();

    Transaction tx = db.createTransaction();

    // List all subdirectories under root ("tables"), these are the individual tables
    List<String> tableDirs = rootDir.list(db).join();

    for (String tableStr : tableDirs)
    {
      System.out.println("Table: " + tableStr);

      // go to meta subdirectory
      List<String> path = new ArrayList<>();
      path.add(tableStr);
      path.add("meta");
      DirectorySubspace metaDir = rootDir.open(db, path).join();

      // get range
      Range r = metaDir.range();
      // iterate over key-value pairs in this range and make TableMetadata object from it
      List<KeyValue> keyValues = tx.getRange(r).asList().join();
      for (KeyValue kv : keyValues)
      {
        // use Tuple api to transform bytes to key and value tuples
        Tuple keyTuple = Tuple.fromBytes(kv.getKey());
        Tuple valueTuple = Tuple.fromBytes(kv.getValue());

        // structure (tableName, tableValue)
        System.out.println("attribute name:" + (String)keyTuple.get(0));
        TableMetadata tbm = new TableMetadata();
      }
    }

    System.out.println("Done with ListTables");


    return null;
    //return tables;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {

    System.out.println("Running addAttribute");
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

    // Tuple keyTuple = new Tuple();
    //keyTuple = keyTuple.add()
    System.out.println("Done with addAttribute");
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    System.out.println("Running dropAttribute");
//    TableMetadata value = tables.get(tableName);
//    // check if table exists
//    if (value == null)
//    {
//      return StatusCode.TABLE_NOT_FOUND;
//    }
//    // check attribute name/type
//    if (attributeName == "")
//    {
//      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
//    }
//    // attribute already exists
//    HashMap<String, AttributeType> attributes = value.getAttributes();
//    if (attributes.containsKey(attributeName))
//    {
//      return StatusCode.ATTRIBUTE_NOT_FOUND;
//    }
//    else
//    {
//      // remove from db
//      try(Database db = fdb.open()) {
//        Transaction transaction = db.createTransaction();
//        Tuple tuple = Tuple.from(tableName);
//        transaction.clear(tuple.range());
//        transaction.commit();
//      }
//      catch (Exception e)
//      {
//        System.out.println(e);
//      }
//      // remove from local
//      attributes.remove(attributeName);
      System.out.println("Done with dropAttribute");
      return StatusCode.SUCCESS;
    //}
  }

  @Override
  public StatusCode dropAllTables() {
//    // clear all inner tables
//    for (Map.Entry<String, TableMetadata> entry: tables.entrySet()){
//      deleteTable(entry.getKey());
//    }
//
//    tables.clear();

    return StatusCode.SUCCESS;
  }
}
