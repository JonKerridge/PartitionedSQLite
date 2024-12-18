package bulkOperations

import groovy.sql.Sql

String url = "jdbc:sqlite:C:\\sqlite\\bulkTests\\bulkDB_1M_1PS_1p.db"
Sql sql = Sql.newInstance(url)

//String attachp2 = """attach database 'c:/sqlite/bulkTests/bulkDB_1M_2PS_2p.db' as p2"""
//
//String createTable = """
//create temp table t as
// select * from bulkData
//  union
// select * from p2.bulkData
//"""

String select = """Select avg(col4), count(*), col3 from bulkData group by col3"""

//sql.execute(attachp2)
//sql.execute(createTable)
List rows = sql.rows(select)
rows.each{r ->
  println "${r[0]}, ${r[1]}, ${r[2]}"
}

sql.close()