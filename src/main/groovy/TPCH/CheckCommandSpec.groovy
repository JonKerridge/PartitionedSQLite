package TPCH

List <List> buildData
buildData = []
String sourceDataFolder = "D:/sqlite/tpch/sourceData/"
String specFileName = "commands.spec"
File specFile = new File("${sourceDataFolder}${specFileName}")
if (!specFile.exists())
  println "spec file ${sourceDataFolder}${specFileName} does not exist"
else {
  specFile.withObjectInputStream {inStream ->
    inStream.eachObject {buildData << it}
  }
}
// print out the spec
buildData.each {println "$it"}