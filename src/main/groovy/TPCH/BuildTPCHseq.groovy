package TPCH

import groovy.sql.Sql
String url = 'jdbc:sqlite:d:/sqlite/tpch/tpch.db'
Sql sql = Sql.newInstance(url)
// directory holding the .tbl files used to populate the database
String directory = "C:\\Users\\JonKe\\OneDrive - Edinburgh Napier University\\Project 2024-5\\TPC-H-Tool\\TPC-H V3.0.1\\dbgen\\"
// the file names for each table
String regionFile = directory + "region.tbl"
String nationFile = directory + "nation.tbl"
String supplierFile = directory + "supplier.tbl"
String partFile = directory + "part.tbl"
String customerFile = directory + "customer.tbl"
String partSuppFile = directory + "partsupp.tbl"
String ordersFile = directory + "orders.tbl"
String lineItemFile = directory + "lineitem.tbl"

// drop all tables if building afresh
String dropRegion = """
DROP TABLE IF EXISTS REGION
"""
String dropNation = """
DROP TABLE IF EXISTS NATION
"""
String dropSupplier = """
DROP TABLE IF EXISTS SUPPLIER
"""
String dropPart = """
DROP TABLE IF EXISTS Part
"""
String dropCustomer = """
DROP TABLE IF EXISTS CUSTOMER
"""
String dropPartSupp = """
DROP TABLE IF EXISTS PARTSUPP
"""
String dropOrders = """
DROP TABLE IF EXISTS ORDERS
"""
String dropLineItem = """
DROP TABLE IF EXISTS LINEITEM
"""

// pragma settings
String walOn = """PRAGMA journal_mode=WAL"""
String walOff = """PRAGMA journal_mode=DELETE"""

// create the tables
String createRegion = """
CREATE TABLE If NOT EXISTS REGION (
   R_REGIONKEY          integer              not null,
   R_NAME               char(25)             null,
   R_COMMENT            varchar(152)         null,
   constraint PK_REGION primary key (R_REGIONKEY)
)
"""

String createNation = """
CREATE TABLE If NOT EXISTS NATION (
  N_NATIONKEY  INTEGER not null,
  N_NAME       CHAR(25) NOT NULL,
  N_REGIONKEY  INTEGER NOT NULL,
  N_COMMENT    VARCHAR(152),
   constraint PK_NATION primary key (N_NATIONKEY),
   constraint FK_REGION foreign key (N_REGIONKEY)
      references REGION (R_REGIONKEY)  
);
"""

String createCustomer = """
create table CUSTOMER (
   C_CUSTKEY            integer          not null,
   C_NAME               varchar(25)          null,
   C_ADDRESS            varchar(40)          null,
   C_NATIONKEY          integer          null,
   C_PHONE              char(15)             null,
   C_ACCTBAL            decimal(12,2)        null,
   C_MKTSEGMENT         char(10)             null,
   C_COMMENT            varchar(117)         null,
   constraint PK_CUSTOMER primary key (C_CUSTKEY),
   constraint FK_CUSTOMER_NATION foreign key (C_NATIONKEY)
      references NATION (N_NATIONKEY)
)
"""

String createSupplier = """
create table SUPPLIER (
   S_SUPPKEY            integer          not null,
   S_NAME               char(25)             null,
   S_ADDRESS            varchar(40)          null,
   S_NATIONKEY          integer              null,
   S_PHONE              char(15)             null,
   S_ACCTBAL            real                 null,
   S_COMMENT            varchar(101)         null,
   constraint PK_SUPPLIER primary key (S_SUPPKEY),
   constraint FK_SUPPLIER_NATION foreign key (S_NATIONKEY)
      references NATION (N_NATIONKEY)
)
"""

String createPart = """
create table PART (
   P_PARTKEY            integer          not null,
   P_NAME               varchar(55)          null,
   P_MFGR               char(25)             null,
   P_BRAND              char(10)             null,
   P_TYPE               varchar(25)          null,
   P_SIZE               decimal(10)          null,
   P_CONTAINER          char(10)             null,
   P_RETAILPRICE        real        null,
   P_COMMENT            varchar(23)          null,
   constraint PK_PART primary key (P_PARTKEY)
)
"""

String createPartSupp = """
create table PARTSUPP (
   PS_PARTKEY           integer          not null,
   PS_SUPPKEY           integer          not null,
   PS_AVAILQTY          integer          null,
   PS_SUPPLYCOST        real       null,
   PS_COMMENT           varchar(199)         null,
   constraint PK_PARTSUPP primary key (PS_PARTKEY, PS_SUPPKEY),
   constraint FK_PART foreign key (PS_PARTKEY)
      references PART (P_PARTKEY),
   constraint FK_SUPP foreign key (PS_SUPPKEY)
      references SUPPLIER (S_SUPPKEY)
)
"""

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

// insert statements
String insertRegion = """
  insert into REGION values (?,?,?)
"""
String insertNation = """
  insert into NATION values (?,?,?,?)
"""
String insertSupplier = """
  insert into SUPPLIER values (?,?,?,?,?,?,?)
"""
String insertPart = """
  insert into PART values (?,?,?,?,?,?,?,?,?)
"""
String insertCustomer = """
  insert into CUSTOMER values (?,?,?,?,?,?,?,?)
"""
String insertPartSupp = """
  insert into PARTSUPP values (?,?,?,?,?)
"""
String insertOrders = """
  insert into ORDERS values (?,?,?,?,?,?,?,?,?)
"""
String insertLineItem = """
  insert into LINEITEM values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

// change journal mode to write ahead log to improve insert speed
List row
long startTime = System.currentTimeSeconds()
row = sql.rows("PRAGMA journal_mode")
println "JM initially: ${row[0][0]}"
sql.execute(walOn)
row = sql.rows("PRAGMA journal_mode")
println "JM set: ${row[0][0]}"

// drop any existing tables and create

//sql.execute(dropRegion)
//sql.execute(createRegion)
//sql.execute(dropNation)
//sql.execute(createNation)
//sql.execute(dropSupplier)
//sql.execute(createSupplier)
//sql.execute(dropPart)
//sql.execute(createPart)
//sql.execute(dropCustomer)
//sql.execute(createCustomer)
sql.execute(dropPartSupp)
sql.execute(createPartSupp)
sql.execute(dropOrders)
sql.execute(createOrders)
sql.execute(dropLineItem)
sql.execute(createLineItem)

// now load the tables the order IS crucial
//Region
//FileReader reader
//reader = new FileReader(new File(regionFile))
//println "\nRegion"
//int count
//count = 0
//reader.eachLine { line ->
//  List tokens
//  tokens = line.tokenize('|')
//  sql.executeInsert(insertRegion,[Integer.parseInt(tokens[0]),tokens[1],tokens[2]])
//  count++
//  if ((count%100) == 0) print "\r $count "
//}
//print "\r $count"
//Nation
//reader = new FileReader(new File(nationFile))
//println "\nNation"
//count = 0
//reader.eachLine { line ->
//  List tokens
//  tokens = line.tokenize('|')
//  sql.executeInsert(insertNation,[Integer.parseInt(tokens[0]),
//                                  tokens[1],
//                                  Integer.parseInt(tokens[2]),
//                                  tokens[3]])
//  count++
//  if ((count%100) == 0) print "\r $count "
//}
//print "\r $count"
//Supplier
//reader = new FileReader(new File(supplierFile))
//println "\nSupplier"
//count = 0
//reader.eachLine { line ->
//  List tokens
//  tokens = line.tokenize('|')
//  sql.executeInsert(insertSupplier,[Integer.parseInt(tokens[0]),
//                                    tokens[1],
//                                    tokens[2],
//                                    Integer.parseInt(tokens[3]),
//                                    tokens[4],
//                                    Double.parseDouble(tokens[5]),
//                                    tokens[6]
//                                    ])
//  count++
//  if ((count%100) == 0) print "\r $count "
//}
//print "\r $count "

////Part
//reader = new FileReader(new File(partFile))
//println "\nPart"
//count = 0
//reader.eachLine { line ->
//  List tokens
//  tokens = line.tokenize('|')
//  sql.executeInsert(insertPart,[Integer.parseInt(tokens[0]),
//                                    tokens[1],
//                                    tokens[2],
//                                    tokens[3],
//                                    tokens[4],
//                                    Integer.parseInt(tokens[5]),
//                                    tokens[6],
//                                    Double.parseDouble(tokens[7]),
//                                    tokens[8]
//                                ])
//  count++
//  if ((count%100) == 0) print "\r $count "
//}
//print "\r $count "

//Customer
//reader = new FileReader(new File(customerFile))
//println "\nCustomer"
//count = 0
//reader.eachLine { line ->
//  List tokens
//  tokens = line.tokenize('|')
//  sql.executeInsert(insertCustomer,[Integer.parseInt(tokens[0]),
//                                tokens[1],
//                                tokens[2],
//                                Integer.parseInt(tokens[3]),
//                                tokens[4],
//                                Double.parseDouble(tokens[5]),
//                                tokens[6],
//                                tokens[7]
//  ])
//  count++
//  if ((count%500) == 0) print "\r $count "
//}
//print "\r $count "


//PartSupp
reader = new FileReader(new File(partSuppFile))
println "\nPartSupp"
count = 0
reader.eachLine { line ->
  List tokens
  tokens = line.tokenize('|')
  sql.executeInsert(insertPartSupp,[Integer.parseInt(tokens[0]),
                                    Integer.parseInt(tokens[1]),
                                    Integer.parseInt(tokens[2]),
                                    Double.parseDouble(tokens[3]),
                                    tokens[4]
  ])
  count++
  if ((count%1000) == 0) print "\r $count "
}
print "\r $count "


sql.execute(walOff)
row = sql.rows("PRAGMA journal_mode")
println "\nJM finally: ${row[0][0]}"

long endTime = System.currentTimeSeconds()
sql.close()
println "Elapsed time = ${endTime - startTime} seconds"
