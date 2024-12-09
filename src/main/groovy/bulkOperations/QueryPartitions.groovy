package bulkOperations

import groovy.sql.Sql

int factor = 10000
String url = "jdbc:sqlite:C:\\sqlite\\bulkTests\\bulkDataBase${factor}k1p.db"
Sql sql = Sql.newInstance(url)

String dropTable = "drop table if exists t"


String createTable = """
create temp table t as
 select id, col4 from bulkData
  union
 select id, col4 from p2.bulkData
  union
 select id, col4 from p3.bulkData
  union
 select id, col4 from p4.bulkData
  union
 select id, col4 from p5.bulkData
  union
 select id, col4 from p6.bulkData
  union
 select id, col4 from p7.bulkData
  union
 select id, col4 from p8.bulkData
  union
 select id, col4 from p9.bulkData
  union
 select id, col4 from p10.bulkData
"""

String index = """create index tIndex on t(id)"""

String select = """Select col4, count(*) from t group by col4"""

long startTime = System.currentTimeSeconds()

sql.execute(dropTable)
for ( i in 2 .. 10){
  String attachTable = "attach database 'c:\\sqlite\\bulkTests\\bulkDataBase${factor}k${i}p.db' as p${i}"
  sql.execute(attachTable)
}

sql.execute(createTable)
sql.execute(index)

List rows = sql.rows(select)
rows.each{r ->
  println "${r[0]}, ${r[1]}"
}

sql.close()

long endTime = System.currentTimeSeconds()
println "Elpased time = ${endTime - startTime} seconds"
