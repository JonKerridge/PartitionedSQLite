import groovy.sql.Sql

String url = 'jdbc:sqlite:c:/sqlite/chinook/chinook.db'
Sql sql = Sql.newInstance(url)
println "connection made to $url"
String query1 = """
SELECT Name, Title FROM albums, artists 
WHERE albums.Artistid = artists.Artistid
"""
List albumArtistList = sql.rows(query1 )
albumArtistList.each {rowV -> println "${rowV.getProperty("Name")}:-  ${rowV.getProperty("Title")}"}
sql.close()
