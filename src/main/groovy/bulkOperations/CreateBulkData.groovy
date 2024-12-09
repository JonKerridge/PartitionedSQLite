package bulkOperations

Random random = new Random()

def randStr(random, length) {
  return random.with { (1..length)
      .collect { (('A'..'Z')+('0'..'9')+('a'..'z'))
          .join()[ nextInt( (('A'..'Z')+('0'..'9')+('a'..'z'))
          .join().length() ) ] }
      .join() }
}

def randInt(random, maxSize) {
  return Math.abs(random.nextInt() % maxSize) + 1
}

def randReal(random, maxValue){
  return (random.nextFloat() * maxValue).round(2)
}

println "test string = ${randStr(random, 20)}"
println "test integer = ${randInt(random, 20)}"
println "test real = ${randReal(random, 20)}"


String bulkDataFolder = "C:\\sqlite\\bulkTests\\"
String bulkFilePrefix = "bulkTest"

int m =1000000
for (int factor in [2,4,8]) {
  int nRows = m * factor
  String fileName = bulkDataFolder + bulkFilePrefix + "${factor}m.txt"
  println "Creating file $fileName"
  long startTime = System.currentTimeSeconds()
  FileWriter writer = new FileWriter(new File(fileName))
  for (i in 1..nRows) {
    String rowValues
    rowValues = """${i},${randStr(16)},${randNum(20)},${randNum(5)}"""
    writer.println(rowValues)
  }
  writer.flush()
  writer.close()
  long endTime = System.currentTimeSeconds()
  println "CSV txt file for factor ${factor} created in ${endTime - startTime} seconds"
}