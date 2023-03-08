
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import jdk.net.SocketFlow;
import org.w3c.dom.Attr;

import java.security.Key;
import java.sql.Array;
import java.util.*;
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
  // helper functions
  public boolean tableExists(String tableName)
  {
    try {
      List<String> tableNames = rootDir.list(db).join();
      boolean found = false;

      for (String name : tableNames)
      {
        if (tableName.equals(name))
        {
          found = true;
          break;
        }
      }
      return found;
    }
    catch (Exception e)
    {
      System.out.println("Error when finding table!: " + e);
    }

    return false;
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
    // System.out.println("Running createTable test");

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

    int transactionCount = 3;
    Transaction tx = db.createTransaction();
    //completeKey = completeKey.add(0).add(primaryKeyAttributeNames[0]);
    for (int i = 0; i < attributeNames.length; i++)
    {
      // resets transaction
      if (transactionCount <= 0)
      {
        tx.commit().join();
        tx.close();
        tx = db.createTransaction();
        transactionCount = 3;
      }

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

      transactionCount--;
    }

    //System.out.println(tableName + " table created successfully!");

    // commit transaction
    tx.commit().join();
    tx.close();

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // check if table exists
    List<String> tableNames = rootDir.list(db).join();
    boolean found = false;

    for (String name : tableNames)
    {
      if (tableName.equals(name))
      {
        found = true;
        break;
      }
    }

    if (!found)
      return StatusCode.TABLE_NOT_FOUND;

     // start deleting
    final DirectorySubspace tableDir = rootDir.open(db, PathUtil.from(tableName)).join();

    Range r = tableDir.range();

    // System.out.println("Running deleteTable");
    Transaction tx = db.createTransaction();
    tx.clear(r);
    tx.commit().join();

    rootDir.remove(db, PathUtil.from(tableName)).join();

    tx.close();

    return StatusCode.SUCCESS;

  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // initialize HashMap to return
    HashMap<String, TableMetadata> result = new HashMap<>();

    Transaction tx = db.createTransaction();

    // List all subdirectories under root ("tables"), these are the individual tables
    List<String> tableDirs = rootDir.list(db).join();

    for (String tableStr : tableDirs)
    {
      // System.out.println("Table: " + tableStr);

      // initialize TableMetaData properties, to be converted to arrays later
      List<String> attributeNames = new ArrayList<>();
      List<AttributeType> attributeTypes = new ArrayList<>();
      List<String> primaryKeyAttributeNames = new ArrayList<>();

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
        {
          // use Tuple api to transform bytes to key and value tuples
          Tuple keyTuple = Tuple.fromBytes(kv.getKey());
          Tuple valueTuple = Tuple.fromBytes(kv.getValue());

          List<Object> keyItems = keyTuple.getItems();
          List<Object> valueItems = valueTuple.getItems();

          attributeNames.add((String)keyItems.get(1));
          attributeTypes.add(AttributeType.valueOf((String) keyItems.get(2)));

/*          System.out.println("attrName: " + keyItems.get(1));
          System.out.println("value tuple size: " + valueItems.size());*/
          // check if primary key attribute
          if ((Boolean) valueItems.get(0))
          {
            primaryKeyAttributeNames.add((String)keyItems.get(1));
          }

/*          System.out.println("printing key obj 0: " + keyItems.get(0));
          System.out.println("printing key obj 1: " + keyItems.get(1));
          System.out.println("printing key obj 2: " + keyItems.get(2));

          System.out.println("val obj 0: " + valueItems.get(0));*/
        }
      }

      if (!attributeNames.isEmpty() && !attributeNames.isEmpty() && !primaryKeyAttributeNames.isEmpty())
      {
        // convert to arrays
        String[] attrNameArr = attributeNames.toArray(new String[attributeNames.size()]);
        AttributeType[] attrTypeArr = attributeTypes.toArray(new AttributeType[attributeTypes.size()]);

        // add in reverse order
        String[] primKeyAttrNamesArr = new String[primaryKeyAttributeNames.size()];
        int bigness = primaryKeyAttributeNames.size();
        for (int i = 0; i < bigness; i++)
        {
          primKeyAttrNamesArr[i] = primaryKeyAttributeNames.get(bigness - 1 - i);
        }
                //primaryKeyAttributeNames.toArray(new String[primaryKeyAttributeNames.size()]);
        //

        // make TableMetadata object
        TableMetadata tbm = new TableMetadata(attrNameArr, attrTypeArr, primKeyAttrNamesArr);
        //System.out.println(tbm);
        result.put(tableStr, tbm);
      }
      //System.out.println()

    }

    // System.out.println("Done with ListTables, size: " + result.size());

    tx.close();

    return result;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {

    //System.out.println("Running addAttribute");
    // check if table exists
    List<String> tableNames = rootDir.list(db).join();
    boolean found = false;

    for (String name : tableNames)
    {
      if (tableName.equals(name))
      {
        found = true;
        break;
      }
    }

    if (!found)
      return StatusCode.TABLE_NOT_FOUND;


    // check if attribute already exists
    List<String> path = new ArrayList<>();
    path.add(tableName);
    path.add("meta");
    DirectorySubspace metaDir = rootDir.open(db, path).join();

    Transaction trans = db.createTransaction();
    List<KeyValue> keyValues = trans.getRange(metaDir.range()).asList().join();

    boolean foundAttribute = false;
    for (KeyValue kv : keyValues)
    {
      Tuple keyTuple = Tuple.fromBytes(kv.getKey());
      List<Object> keyItems = keyTuple.getItems();

      String name = (String)keyItems.get(1);
      if (name.equals(attributeName))
      {
        foundAttribute = true;
        break;
      }
    }

    if (foundAttribute)
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;

    // add attribute if not found

    Transaction tx = db.createTransaction();

    // key tuples
    Tuple keyTuple = new Tuple();
    Tuple valueTuple = new Tuple();

    keyTuple = keyTuple.add(attributeName);
    keyTuple = keyTuple.add(attributeType.name());

    // assumes added one cannot be primaryKey
    boolean var = false;
    valueTuple = valueTuple.add(var);

    tx.set(metaDir.pack(keyTuple), valueTuple.pack());

    tx.commit().join();
    tx.close();

    //System.out.println("Done with addAttribute");
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {

    // System.out.println("Running dropAttribute");
    // check if table exists
    List<String> tableNames = rootDir.list(db).join();
    boolean found = false;

    for (String name : tableNames)
    {
      if (tableName.equals(name))
      {
        found = true;
        break;
      }
    }

    if (!found)
      return StatusCode.TABLE_NOT_FOUND;

      // begin dropping attribute
      Transaction tx = db.createTransaction();

      List<String> path = new ArrayList<>();
      path.add(tableName);
      path.add("meta");
      DirectorySubspace metaDir = rootDir.open(db, path).join();

      List<KeyValue> keyValues = tx.getRange(metaDir.range()).asList().join();

      boolean foundAttribute = false;
      for (KeyValue kv : keyValues)
      {
        Tuple keyTuple = Tuple.fromBytes(kv.getKey());
        List<Object> keyItems = keyTuple.getItems();
        // List<Object> valueItems = valueTuple.getItems();
        String name = (String)keyItems.get(1);
        if (name.equals(attributeName))
        {
          // clear found attribute
          tx.clear(kv.getKey());
          foundAttribute = true;
        }
      }

      if (!foundAttribute)
        return StatusCode.ATTRIBUTE_NOT_FOUND;
      // List<String> key = rootDir.list(db).join();

      //System.out.println("Done with dropAttribute");
      return StatusCode.SUCCESS;
    //}
  }

  @Override
  public StatusCode dropAllTables() {
    // remove directories
   List<String> tableNames = rootDir.list(db).join();

    for (String name : tableNames)
    {
      rootDir.remove(db, PathUtil.from(name)).join();
      //deleteTable(name);
    }

    // clear keys
    Transaction tx = db.createTransaction();
    tx.clear(rootDir.range());
    tx.commit().join();

    return StatusCode.SUCCESS;
  }
}
