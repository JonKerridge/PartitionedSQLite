package TPCH

String sourceDataFolder = "D:/sqlite/tpch/sourceData/"
String customerData = "customer"
String lineItemData = "lineitem"
String nationData = "nation"
String ordersData = "orders"
String partData = "part"
String partSuppData = "partsupp"
String regionData = "region"
String supplierData = "supplier"

List fileNames = ["${sourceDataFolder}${regionData}.tbl",
                           "${sourceDataFolder}${nationData}.tbl",
                           "${sourceDataFolder}${supplierData}.tbl",
                           "${sourceDataFolder}${partData}.tbl",
                           "${sourceDataFolder}${partSuppData}.tbl",
                           "${sourceDataFolder}${customerData}.tbl",
                           "${sourceDataFolder}${ordersData}.tbl",
                           "${sourceDataFolder}${lineItemData}.tbl" ]

String dataFileDelimiter = '|'

String databaseRootURL = "C:/sqlite/tpch/"

// table drop statements not needed on initial load
//String dropRegion = """DROP TABLE IF EXISTS REGION"""
//String dropNation = """DROP TABLE IF EXISTS NATION"""
//String dropSupplier = """DROP TABLE IF EXISTS SUPPLIER"""
//String dropPart = """DROP TABLE IF EXISTS PART"""
//String dropCustomer = """DROP TABLE IF EXISTS CUSTOMER"""
//String dropPartSupp = """DROP TABLE IF EXISTS PARTSUPP"""
//String dropOrders = """DROP TABLE IF EXISTS ORDERS"""
//String dropLineItem = """DROP TABLE IF EXISTS LINEITEM"""
//
//List dropList = [dropRegion, dropNation, dropSupplier, dropPart,
//                         dropPartSupp, dropCustomer, dropOrders, dropLineItem]

// create the tables
String createRegion = """
CREATE TABLE If NOT EXISTS REGION (
   R_REGIONKEY          integer  primary key,
   R_NAME               char(25)             null,
   R_COMMENT            varchar(152)         null,
)
"""

String createNation = """
CREATE TABLE If NOT EXISTS NATION (
  N_NATIONKEY  INTEGER primary key,
  N_NAME       CHAR(25) NOT NULL,
  N_REGIONKEY  INTEGER NOT NULL,
  N_COMMENT    VARCHAR(152),
  constraint FK_REGION foreign key (N_REGIONKEY)
      references REGION (R_REGIONKEY)  
)
"""

String createCustomer = """
create table CUSTOMER (
   C_CUSTKEY            integer  primary key,
   C_NAME               varchar(25)          null,
   C_ADDRESS            varchar(40)          null,
   C_NATIONKEY          integer          null,
   C_PHONE              char(15)             null,
   C_ACCTBAL            decimal(12,2)        null,
   C_MKTSEGMENT         char(10)             null,
   C_COMMENT            varchar(117)         null,
   constraint FK_CUSTOMER_NATION foreign key (C_NATIONKEY)
      references NATION (N_NATIONKEY)
)
"""

String createSupplier = """
create table SUPPLIER (
   S_SUPPKEY            integer  primary key,
   S_NAME               char(25)             null,
   S_ADDRESS            varchar(40)          null,
   S_NATIONKEY          integer              null,
   S_PHONE              char(15)             null,
   S_ACCTBAL            real                 null,
   S_COMMENT            varchar(101)         null,
   constraint FK_SUPPLIER_NATION foreign key (S_NATIONKEY)
      references NATION (N_NATIONKEY)
)
"""

String createPart = """
create table PART (
   P_PARTKEY            integer  primary key,
   P_NAME               varchar(55)          null,
   P_MFGR               char(25)             null,
   P_BRAND              char(10)             null,
   P_TYPE               varchar(25)          null,
   P_SIZE               decimal(10)          null,
   P_CONTAINER          char(10)             null,
   P_RETAILPRICE        real        null,
   P_COMMENT            varchar(23)          null,
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
   O_ORDERKEY           integer  primary key,
   O_CUSTKEY            integer          null,
   O_ORDERSTATUS        char(1)              null,
   O_TOTALPRICE         real        null,
   O_ORDERDATE          text             null,
   O_ORDERPRIORITY      char(15)             null,
   O_CLERK              char(15)             null,
   O_SHIPPRIORITY       integer          null,
   O_COMMENT            varchar(79)          null,
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

List  createList = [createRegion, createNation, createSupplier, createPart,
                            createPartSupp, createCustomer, createOrders, createLineItem]

// insert statements
String insertRegion = """insert into REGION values (?,?,?)"""
String insertNation = """insert into NATION values (?,?,?,?)"""
String insertSupplier = """insert into SUPPLIER values (?,?,?,?,?,?,?)"""
String insertPart = """insert into PART values (?,?,?,?,?,?,?,?,?)"""
String insertCustomer = """insert into CUSTOMER values (?,?,?,?,?,?,?,?)"""
String insertPartSupp = """insert into PARTSUPP values (?,?,?,?,?)"""
String insertOrders = """insert into ORDERS values (?,?,?,?,?,?,?,?,?)"""
String insertLineItem = """insert into LINEITEM values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

List insertList = [insertRegion, insertNation, insertSupplier, insertPart,
                          insertPartSupp, insertCustomer, insertOrders, insertLineItem]

// index drop and creation
// these are all rimary key indexes and so are created
// automatically by SQLite
//String dropRindex = """drop index if exists R_INDEX"""
//String createRIndex = """create unique index R_INDEX on REGION(R_REGIONKEY)"""
//String dropNindex = """drop index if exists N_INDEX"""
//String createNIndex = """create unique index N_INDEX on NATION(N_NATIONKEY)"""
//String dropSindex = """drop index if exists S_INDEX"""
//String createSIndex = """create unique index S_INDEX on SUPPLIER(S_SUPPKEY)"""
//String dropPindex = """drop index if exists P_INDEX"""
//String createPIndex = """create unique index P_INDEX on PART(P_PARTKEY)"""
//String dropPSindex = """drop index if exists PS_INDEX"""
//String createPSIndex = """create unique index PS_INDEX on PARTSUPP(PS_PARTKEY, PS_SUPPKEY)"""
//String dropCindex = """drop index if exists C_INDEX"""
//String createCIndex = """create unique index C_INDEX on CUSTOMER(C_CUSTKEY)"""
//String dropOindex = """drop index if exists O_INDEX"""
//String createOIndex = """create unique index O_INDEX on ORDERS(O_ORDERKEY)"""
//String dropLIindex = """drop index if exists L_INDEX"""
//String createLIindex = """create unique index L_INDEX on LINEITEM(L_ORDERKEY, L_LINENUMBER)"""
//
//List  dropIndexList = [dropRindex, dropNindex, dropSindex, dropPindex,
//                              dropPSindex, dropCindex, dropOindex, dropLIindex]
//List  createIndexList =[createRIndex, createNIndex, createSIndex, createPIndex,
//                               createPSIndex, createCIndex, createOIndex, createLIindex]
// table column data types
List <String> dataTypesRegion = ['int', 'text', 'text']
List <String> dataTypesNation = ['int', 'text', 'int', 'text' ]
List <String> dataTypesSupplier = ['int', 'text', 'text', 'int', 'text', 'real', 'text']
List <String> dataTypesPart = ['int', 'text', 'text', 'text', 'text', 'int', 'text', 'real', 'text']
List <String> dataTypesPartSupp = ['int', 'int', 'int', 'real', 'text']
List <String> dataTypesCustomer = ['int', 'text', 'text', 'int', 'text', 'real', 'text', 'text']
List <String> dataTypesOrders = ['int', 'int', 'text', 'real', 'text', 'text', 'text', 'int', 'text']
List <String> dataTypesLineItem = ['int', 'int', 'int', 'int', 'real', 'real', 'real', 'real',
                                   'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text']

List typeList = [dataTypesRegion, dataTypesNation, dataTypesSupplier, dataTypesPart,
                                dataTypesPartSupp, dataTypesCustomer, dataTypesOrders, dataTypesLineItem]

// no SQLite table can be partitioned

// partitions per table
//int partitionsRegion = 1
//int partitionsNation = 1
//int partitionsSupplier = 1
//int partitionsPart = 2
//int partitionsPartSupp = 4
//int partitionsCustomer = 2
//int partitionsOrders = 15
//int partitionsLineItem = 61
//
//List <Integer> partsPerTable  = [partitionsRegion, partitionsNation, partitionsSupplier, partitionsPart,
//                                 partitionsPartSupp, partitionsCustomer, partitionsOrders, partitionsLineItem]

// partition sizes -1 indicates table not partitioned
//int partitionSizeRegion = -1
//int partitionSizeNation = -1
//int partitionSizeSupplier = -1
//int partitionSizePart = 100000
//int partitionSizePartSupp = 200000
//int partitionSizeCustomer = 75000
//int partitionSizeOrders = 100000
//int partitionSizeLineItem = 100000

//List <Integer> partitionSizes = [partitionSizeRegion, partitionSizeNation, partitionSizeSupplier, partitionSizePart,
//                                 partitionSizePartSupp, partitionSizeCustomer, partitionSizeOrders,partitionSizeLineItem]

// SQLite PRAGMA settings
String jmOff = "PRAGMA journal_mode = OFF"
String synch = "PRAGMA synchronous = 0"
String cache = "PRAGMA cache_size = 1000000"
String lock = "PRAGMA locking_mode = EXCLUSIVE"
String temp = "PRAGMA temp_store = MEMORY"

List <String> pragmas = [ jmOff, synch, cache, lock, temp]

//
int nPragmaStatements = 5
int nTableCreates = 8
//int nIndexBuilds =  8  //not required in SQLite

// now build the LIst structure
List <List> buildData
buildData  = []
List buildLine
buildLine = [nPragmaStatements, nTableCreates, dataFileDelimiter, databaseRootURL]
buildData << buildLine
buildLine = []
for (i in 0 ..< nPragmaStatements) buildLine << pragmas[i]
buildData << buildLine
buildLine = []
for (i in 0 ..< nTableCreates){
  buildLine << fileNames[i]
//  buildLine << dropList[i] not for initial load
  buildLine << createList[i]
  buildLine << insertList[i]
  buildLine << typeList[i]
  // sqlite tables cannot be partitioned
//  buildLine << partsPerTable[i]
//  buildLine << partitionSizes[i]
  buildData << buildLine
  buildLine = []
}
// now the indexes but SQLite builds primary key indexes automatically
//for (i in 0 ..< nIndexBuilds){
//  buildLine << dropIndexList[i]
//  buildLine << createIndexList[i]
//  buildData << buildLine
//  buildLine = []
//}

buildData.each{println "\n$it"}

String commandFileTextName = sourceDataFolder + 'commands.txt'
File commandFileText = new File (commandFileTextName)
if (commandFileText.exists()) commandFileText.delete()
PrintWriter commandWriter = new PrintWriter(commandFileText)
String commandFileName = sourceDataFolder + 'commands.spec'
File commandFile = new File (commandFileName)
if (commandFile.exists()) commandFile.delete()
ObjectOutputStream commandStream = commandFile.newObjectOutputStream()
buildData.each{
    commandWriter.println "$it"
    commandStream << it
}
commandStream.flush()
commandStream.close()
commandWriter.flush()
commandWriter.close()
println "finished building command file $commandFileName"
