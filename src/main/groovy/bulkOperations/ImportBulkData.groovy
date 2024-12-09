package bulkOperations

import groovy.sql.Sql

int factor = 1000
String url = "jdbc:sqlite:C:\\sqlite\\bulkTests\\bulkData${factor}k.db"
Sql sql = Sql.newInstance(url)
String dataFile = "C:\\sqlite\\bulkTests\\bulkData${factor}k.txt"

String createTable = """
CREATE TABLE bulkData${factor}k (
id INTEGER PRIMARY KEY, 
col2 TEXT, 
col3 INTEGER,
col4 INTEGER)
"""

String dropTable = """drop table if exists bulkData${factor}k"""

String insertRow = """
insert into bulkData${factor}k values (?,?,?,?)
"""

String jmOff = "PRAGMA journal_mode = OFF"
String synch = "PRAGMA synchronous = 0"
String cache = "PRAGMA cache_size = 1000000"
String lock = "PRAGMA locking_mode = EXCLUSIVE"
String temp = "PRAGMA temp_store = MEMORY"
int oneK = 1000
int maxRows = factor*oneK
int batchSize = maxRows/10


long startTime = System.currentTimeSeconds()

List <List> rowData
rowData = []
count = 0
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

// create table
sql.execute(dropTable)
sql.execute(createTable)
// now do some optimisations
sql.execute(jmOff)
sql.execute(synch)
sql.execute(cache)
sql.execute(lock)
sql.execute(temp)
// now insert data in batches

sql.withBatch(batchSize, insertRow) {ps ->
  for (i in 0 ..< rowData.size()) {
    ps.addBatch(rowData[i])
  }
}
sql.close()
long endTime = System.currentTimeSeconds()
println "Data added to table in ${endTime-readTime} seconds"

println "\nElapsed time = ${endTime - startTime} seconds"
