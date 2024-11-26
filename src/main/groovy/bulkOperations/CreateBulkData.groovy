package bulkOperations

String bulkDataFolder = "C:\\sqlite\\bulkTests\\"
String bulkFilePrefix = "bulkData"

def randStr(length) {
  return new Random().with { (1..length)
      .collect { (('A'..'Z')+('0'..'9')+('a'..'z'))
          .join()[ nextInt( (('A'..'Z')+('0'..'9')+('a'..'z'))
          .join().length() ) ] }
      .join() }
}

def randNum(maxSize) {
  return Math.abs(new Random().nextInt() % maxSize) + 1
}

int nRows = 10000000
String fileName = bulkDataFolder + bulkFilePrefix +"${nRows/1000}k.txt"
println "Creating file $fileName"
FileWriter writer = new FileWriter(new File(fileName))
for (i in 1 .. nRows) {
  String rowValues
  rowValues = """${i},${randStr(16)},${randNum(20)},${randNum(5)}"""
  writer.println(rowValues)
}
writer.flush()
writer.close()
println "CSV txt file created"