package bulkOperations

import groovy.sql.Sql
import jcsp.lang.CSProcess



class WritePartition implements CSProcess{

  int partitionSize
  int partitionNumber
  String databaseURL
  List rowData

  String dropTable = """drop table if exists bulkData"""

  String createTable = """
    CREATE TABLE bulkData (
    id INTEGER PRIMARY KEY, 
    col2 TEXT, 
    col3 INTEGER,
    col4 INTEGER)
   """

  String insertRow = """
    insert into bulkData values (?,?,?,?)
  """

  String jmOff = "PRAGMA journal_mode = OFF"
  String synch = "PRAGMA synchronous = 0"
  String cache = "PRAGMA cache_size = 1000000"
  String lock = "PRAGMA locking_mode = EXCLUSIVE"
  String temp = "PRAGMA temp_store = MEMORY"

  @Override
  void run() {
    Sql sql = Sql.newInstance(databaseURL)
    // create table within the partition
    sql.execute(dropTable)
    sql.execute(createTable)
    // now do some optimisations
    sql.execute(jmOff)
    sql.execute(synch)
    sql.execute(cache)
    sql.execute(lock)
    sql.execute(temp)
    // now insert data in batches
    int offset
    offset = (partitionNumber - 1) * partitionSize
    sql.withBatch(1000, insertRow) {ps ->
      for (i in 0 ..< partitionSize) {
        ps.addBatch(rowData[offset])
        offset++
      }
    } // with batch
    sql.close()
  } // run
}
