package TPCH

import groovy.sql.Sql

String url = 'jdbc:sqlite:d:/sqlite/tpch/tpch.db'
Sql sql = Sql.newInstance(url)
String directory = "C:\\Users\\JonKe\\OneDrive - Edinburgh Napier University\\Project 2024-5\\TPC-H-Tool\\TPC-H V3.0.1\\dbgen\\"
String ordersFile = directory + "orders.tbl"

String dropOrders = """
DROP TABLE IF EXISTS ORDERS
"""

// pragma settings
String walOn = """PRAGMA journal_mode=WAL"""
String walOff = """PRAGMA journal_mode=DELETE"""

String createOrders = """
create table ORDERS (
   O_ORDERKEY           integer          not null,
   O_CUSTKEY            integer          null,
   O_ORDERSTATUS        char(1)              null,
   O_TOTALPRICE         real        null,
   O_ORDERDATE          text             null,
   O_ORDERPRIORITY      char(15)             null,
   O_CLERK              char(15)             null,
   O_SHIPPRIORITY       integer          null,
   O_COMMENT            varchar(79)          null,
   constraint PK_ORDERS primary key (O_ORDERKEY),
   constraint FK_CUSTOMER foreign key (O_CUSTKEY)
      references CUSTOMER (C_CUSTKEY)
)
"""

String insertOrders = """
  insert into ORDERS values (?,?,?,?,?,?,?,?,?)
"""

long startTime = System.currentTimeSeconds()
sql.execute(walOn)
sql.execute(dropOrders)
sql.execute(createOrders)

// read file into a List of parsed data 1 entry per row
List <List> ordersData
ordersData = []
int count
count = 0
FileReader reader = new FileReader(new File(ordersFile))
reader.eachLine {line ->
  List tokens
  tokens = line.tokenize('|')
  List lineData
  lineData = [Integer.parseInt(tokens[0]),
              Integer.parseInt(tokens[1]),
              tokens[2],
              Double.parseDouble(tokens[3]),
              tokens[4],
              tokens[5],
              tokens[6],
              Integer.parseInt(tokens[7]),
              tokens[8],
             ]
  ordersData << lineData
  count++
  if ((count%1000) == 0) print "\r $count "
}
print "\r $count rows copied to internal memory"
count = 0
sql.withBatch(1000, insertOrders) {ps ->
  for (i in 0 ..< ordersData.size()) {
    ps.addBatch(ordersData[i])
  }
}
println "data added to orders"
sql.execute(walOff)
sql.close()
long endTime = System.currentTimeSeconds()

println "\nElapsed time = ${endTime - startTime} seconds"