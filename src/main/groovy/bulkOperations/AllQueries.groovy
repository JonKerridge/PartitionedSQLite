package bulkOperations

import groovy.sql.Sql
//import org.sqlite.SQLiteLimits

String connectionURL = "jdbc:sqlite:C:\\sqlite\\bulkTests\\"
String select = """Select avg(col4), count(*), col3 from t group by col3"""
String createTable = """
create temp table t as
 select * from bulkData
"""
String attach ="""attach database 'c:/sqlite/bulkTests/bulkDB_"""

for (f in [1,2,4,8]){
  for (p in [1,2,4,8]){
    long startTime = System.currentTimeMillis()
    String connection = connectionURL + "bulkDB_${f}M_${p}PS_1p.db"
    Sql sql = Sql.newInstance(connection)
    //sql.execute(changeLimit)
//    sqlite3_limit(sql, 7, 32)
    if (p == 1){
      sql.execute(createTable)
    } else {
      String createTemp = createTable
      for ( pn in 2 .. p){
        String attachPN = attach +"${f}M_${p}PS_${pn}p.db' as p${pn}"
        createTemp = createTemp + " union select * from p${pn}.bulkData"
        sql.execute(attachPN)
      }
      sql.execute(createTemp)
    } //else
    List rows = sql.rows(select)
    long endTime = System.currentTimeMillis()
    println "Run for f: $f, p: $p in ${endTime - startTime} msecs"
//    rows.each{r ->
//      println "${r[0]}, ${r[1]}, ${r[2]}"
//    }
    sql.close()
  }// end p
}  // end f
