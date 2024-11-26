import groovy.sql.Sql

String url = 'jdbc:sqlite:c:/sqlite/chinook/chinook.db'
Sql sql = Sql.newInstance(url)
println "connection made to $url"
List albumList = sql.rows('SELECT Albumid, Title, Artistid FROM albums')
albumList.each {rowV -> println "${rowV.getProperty("Albumid")},  ${rowV.getProperty("Title")}, ${rowV.getProperty("Artistid")}"}
sql.close()