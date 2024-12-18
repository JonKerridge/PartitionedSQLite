package bulkOperations

Random random = new Random()

def randStr(Random random, int length) {
  return random.with { (1..length)
      .collect { (('A'..'Z')+('0'..'9')+('a'..'z'))
          .join()[ nextInt( (('A'..'Z')+('0'..'9')+('a'..'z'))
          .join().length() ) ] }
      .join() }
}

def randInt(Random random, int maxSize) {
  return Math.abs(random.nextInt() % maxSize) + 1
}

def randReal(Random random, float maxValue){
  return (random.nextFloat() * maxValue).round(2)
}

String bulkDataFolder = "C:\\sqlite\\bulkTests\\"
String bulkFilePrefix = "bulkTest"

int m = 1024 * 1024
for (int factor in [1,2,4,8]) {
  int nRows = m * factor
  String fileName = bulkDataFolder + bulkFilePrefix + "${factor}m.txt"
  println "Creating file $fileName"
  long startTime = System.currentTimeSeconds()
  FileWriter writer = new FileWriter(new File(fileName))
  for (i in 1..nRows) {
    String rowValues
    rowValues = """${i},${randStr(random, 16)},${randInt(random, 20)},${randReal(random, 5.0)}"""
    writer.println(rowValues)
  }
  writer.flush()
  writer.close()
  long endTime = System.currentTimeSeconds()
  println "CSV txt file for factor ${factor} created in ${endTime - startTime} seconds"
}