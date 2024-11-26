package TPCH

import groovy.sql.Sql
String url = 'jdbc:sqlite:d:/sqlite/tpch/tpch.db'
Sql sql = Sql.newInstance(url)
// directory holding the .tbl files used to populate the database
String directory = "C:\\Users\\JonKe\\OneDrive - Edinburgh Napier University\\Project 2024-5\\TPC-H-Tool\\TPC-H V3.0.1\\dbgen\\"
String lineItemFile = directory + "lineitem.tbl"

String dropLineItem = """
DROP TABLE IF EXISTS LINEITEM
"""

String createLineItem = """
create table LINEITEM (
   L_ORDERKEY           integer          not null,
   L_PARTKEY            integer          null,
   L_SUPPKEY            integer         null,
   L_LINENUMBER         integer          not null,
   L_QUANTITY           real        null,
   L_EXTENDEDPRICE      real        null,
   L_DISCOUNT           real        null,
   L_TAX                real        null,
   L_RETURNFLAG         char(1)              null,
   L_LINESTATUS         char(1)              null,
   L_SHIPDATE           text             null,
   L_COMMITDATE         text             null,
   L_RECEIPTDATE        text             null,
   L_SHIPINSTRUCT       char(25)             null,
   L_SHIPMODE           char(10)             null,
   L_COMMENT            varchar(44)          null,
   constraint PK_LINEITEM primary key (L_ORDERKEY, L_LINENUMBER),
   constraint FK_ORDER foreign key (L_ORDERKEY)
      references ORDERS (O_ORDERKEY),
   constraint FK_PARTSUPP foreign key (L_PARTKEY, L_SUPPKEY)
      references PARTSUPP (PS_PARTKEY, PS_SUPPKEY)
)
"""

String insertLineItem = """
  insert into LINEITEM values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

//Pragma statements
String jOff = """PRAGMA journal_mode=off"""
String jOn = """PRAGMA journal_mode=delete"""
String lockMode = """PRAGMA locking_mode= EXCLUSIVE"""

long startTime = System.currentTimeSeconds()
sql.execute(jOff)
//sql.execute(lockMode)
sql.execute(dropLineItem)
sql.execute(createLineItem)

// read file into a List of parsed data 1 entry per row
List <List> lineItemData
lineItemData = []
int count
count = 0
FileReader reader = new FileReader(new File(lineItemFile))
reader.eachLine {line ->
  List tokens
  tokens = line.tokenize('|')
  List lineData
  lineData = [Integer.parseInt(tokens[0]),
              Integer.parseInt(tokens[1]),
              Integer.parseInt(tokens[2]),
              Integer.parseInt(tokens[3]),
              Double.parseDouble(tokens[4]),
              Double.parseDouble(tokens[5]),
              Double.parseDouble(tokens[6]),
              Double.parseDouble(tokens[7]),
              tokens[8],
              tokens[9],
              tokens[10],
              tokens[11],
              tokens[12],
              tokens[13],
              tokens[14],
              tokens[15]
  ]
  lineItemData << lineData
  count++
  if ((count%1000) == 0) print "\r $count "
}
print "\r $count rows copied to internal memory"
count = 0
sql.withBatch(10000, insertLineItem) {ps ->
  for (i in 0 ..< lineItemData.size()) {
    ps.addBatch(lineItemData[i])
  }
}
println "data added to orders"
sql.execute(jOn)

