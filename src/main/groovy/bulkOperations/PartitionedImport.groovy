package bulkOperations

import groovy.sql.Sql

int partitions = 10   //fixed number
int oneK = 1000
int factor = 10000     // or 1000
int partitionSize = factor * oneK / partitions
int batchSize = 1000  //fixed

String directory = "C:\\sqlite\\bulkTests\\"
String dataFile = directory + "bulkData${factor}k.txt"

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

// read in all the data to be imported
long startTime = System.currentTimeSeconds()

List <List> rowData
rowData = []

FileReader reader = new FileReader(new File(dataFile))
reader.eachLine { line ->
  List tokens
  tokens = line.tokenize(',')
  List lineData
  lineData = [
      Integer.parseInt(tokens[0]),
      tokens[1],
      Integer.parseInt(tokens[2]),
      Integer.parseInt(tokens[3])
  ]
  rowData << lineData
}
long readTime = System.currentTimeSeconds()
println "$factor k data copied to internal memory in ${readTime- startTime} seconds"

for ( pn in 1 .. partitions) {
  long startLoad = System.currentTimeSeconds()
  String url = "jdbc:sqlite:C:\\sqlite\\bulkTests\\bulkDataBase${factor}k${pn}p.db"
  println "Creating $url"
  Sql sql = Sql.newInstance(url)
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
  offset = (pn - 1) * partitionSize
  sql.withBatch(batchSize, insertRow) {ps ->
    for (i in 0 ..< partitionSize) {
      ps.addBatch(rowData[offset])
      offset++
    }
  } // with batch
  long endLoad = System.currentTimeSeconds()
  println "Partition $pn written to its database in ${endLoad-startLoad} seconds"
  sql.close()
} // end for

long endTime = System.currentTimeSeconds()
println "\nElapsed time = ${endTime - startTime} seconds"
