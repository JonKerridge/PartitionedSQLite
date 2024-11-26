package TPCH

import groovy.sql.Sql
String url = 'jdbc:sqlite:d:/sqlite/tpch/tpch.db'
Sql sql = Sql.newInstance(url)

// Region
String dropRindex = """drop index if exists R_INDEX"""
String createRIndex = """
create unique index R_INDEX 
on REGION(R_REGIONKEY)
"""

//Nation
String dropNindex = """drop index if exists N_INDEX"""
String createNIndex = """
create unique index N_INDEX 
on NATION(N_NATIONKEY)
"""

//Supplier
String dropSindex = """drop index if exists S_INDEX"""
String createSIndex = """
create unique index S_INDEX 
on SUPPLIER(S_SUPPKEY)
"""

//Part
String dropPindex = """drop index if exists P_INDEX"""
String createPIndex = """
create unique index P_INDEX 
on PART(P_PARTKEY)
"""

//PartSupp
String dropPSindex = """drop index if exists PS_INDEX"""
String createPSIndex = """
create unique index PS_INDEX 
on PARTSUPP(PS_PARTKEY, PS_SUPPKEY)
"""

//Customer
String dropCindex = """drop index if exists C_INDEX"""
String createCIndex = """
create unique index C_INDEX 
on CUSTOMER(C_CUSTKEY)
"""

//Orders
String dropOindex = """drop index if exists O_INDEX"""
String createOIndex = """
create unique index O_INDEX 
on ORDERS(O_ORDERKEY)
"""

//LineItem
String dropLindex = """drop index if exists L_INDEX"""
String createLIndex = """
create unique index L_INDEX 
on LINEITEM(L_ORDERKEY, L_LINENUMBER)
"""

//build the indexes
long startTime = System.currentTimeSeconds()

// drop any existing indexes
//sql.execute(dropRindex)
//sql.execute(dropNindex)
//sql.execute(dropSindex)
//sql.execute(dropPindex)
//sql.execute(dropPSindex)
//sql.execute(dropCindex)
//sql.execute(dropOindex)
sql.execute(dropLindex)

// create new indexes
//sql.execute(createRIndex)
//sql.execute(createNIndex)
//sql.execute(createSIndex)
//sql.execute(createPIndex)
//sql.execute(createPSIndex)
//sql.execute(createCIndex)
//sql.execute(createOIndex)
sql.execute(createLIndex)


long endTime = System.currentTimeSeconds()
println "Indexes built"
sql.close()
println "Elapsed time = ${endTime - startTime} seconds"
