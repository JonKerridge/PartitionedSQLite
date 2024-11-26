package bulkOperations

import groovy.sql.Sql

def randStr(length) {
  return new Random().with { (1..length)
      .collect { (('A'..'Z')+('0'..'9')+('a'..'z'))
          .join()[ nextInt( (('A'..'Z')+('0'..'9')+('a'..'z'))
          .join().length() ) ] }
      .join() }
}

def randNum(maxSize) {
  return Math.abs(new Random().nextInt() % maxSize) + 1
}
String url = 'jdbc:sqlite:c:/sqlite/bulkTest.db'
Sql sql = Sql.newInstance(url)
String createTable = """
CREATE TABLE bulkData (
id INTEGER PRIMARY KEY, 
col2 TEXT, 
col3 INTEGER,
col4 INTEGER)
"""

String dropTable = """drop table if exists bulkData"""

String insertRow = """
insert into bulkData values (?,?,?,?)
"""

String jmOff = "PRAGMA journal_mode = OFF"
String synch = "PRAGMA synchronous = 0"
String cache = "PRAGMA cache_size = 1000000"
String lock = "PRAGMA locking_mode = EXCLUSIVE"
String temp = "PRAGMA temp_store = MEMORY"

int maxRows = 100000
int batchSize = 1000
int count

long startTime = System.currentTimeSeconds()

List <List> rowData
rowData = []

for (i in 0 ..< maxRows){
  List rowValues
  rowValues = [
      i+1,
      randStr(16),
      randNum(20),
      randNum(5)
  ]
  rowData << rowValues
}
long createTime = System.currentTimeSeconds()
println "Row Values created in ${createTime - startTime} seconds\n adding batches\n"

count = 1
sql.execute(dropTable)
sql.execute(createTable)
// now do some optimisations
sql.execute(jmOff)
sql.execute(synch)
sql.execute(cache)
sql.execute(lock)
sql.execute(temp)
// now insert data in batches
sql.withBatch (batchSize, insertRow){ ps ->
  for ( i in 0 ..< maxRows){
    ps.addBatch(rowData[i])
    if (i%batchSize == 0) {
      print "\r$count"
      count++
    }
  }
}
println "\n ${count-1} batches added"
sql.close()
long endTime = System.currentTimeSeconds()

println "\nLoad time = ${endTime - createTime} seconds"
