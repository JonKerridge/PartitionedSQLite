package bulkOperations

import groovy_jcsp.PAR
import jcsp.lang.CSProcess

int oneM = 1024 * 1024

String directory = "C:\\sqlite\\bulkTests\\"
String dataFile
List <CSProcess> writers
List <List> rowData
List<String> results
results = []
for ( f in [1,2,4,8])   results << "Read,${f},,"
for (f in [1,2,4,8]){
  for (p in [1,2,4,8,16])
    results << "Write,${f},${p},"
}

for ( r in 1 .. 5) { // 5 runs
  println "Run $r"
  for (f in [1, 2, 4, 8]) {
    // read in all the data to be imported
    long startTime = System.currentTimeMillis()
    dataFile = directory + "bulkTest${f}m.txt"
    rowData = []
    FileReader reader = new FileReader(new File(dataFile))
    reader.eachLine { line ->
      List tokens
      tokens = line.tokenize(',')
      List lineData
      lineData = [Integer.parseInt(tokens[0]),
                  tokens[1],
                  Integer.parseInt(tokens[2]),
                  Float.parseFloat(tokens[3])]
      rowData << lineData
    }
    long readTime = System.currentTimeMillis()
    println "Read size ${f}"
    int readRow, writeOffset, writeRow
    switch (f) {
      case 1:
        readRow = 0
        writeOffset = 4
        break
      case 2:
        readRow = 1
        writeOffset = 9
        break
      case 4:
        readRow = 2
        writeOffset = 14
        break
      case 8:
        readRow = 3
        writeOffset = 19
        break
    }
    results[readRow] = results[readRow] + "${readTime - startTime},"
    for (p in [1, 2, 4, 8, 16]) {  // number of partitions
      long startLoad = System.currentTimeMillis()
      writers = []
      int partitionSize = (int) (f * oneM / p)
      for (pn in 1..p) {
        String url = "jdbc:sqlite:C:\\sqlite\\bulkTests\\bulkDB_${f}M_${p}PS_${pn}p.db"
        WritePartition wp = new WritePartition(partitionSize: partitionSize,
            partitionNumber: pn,
            databaseURL: url,
            rowData: rowData)
        writers << wp
      } // end for partitions
      new PAR(writers).run()
      long endLoad = System.currentTimeMillis()
//    println "bulkDB_${f}M_${p}PS partition(s) written to databases in ${endLoad - startLoad} seconds"
//    println "Write, $f, $p, ${endLoad - startLoad}"
      switch (p) {
        case 1:
          writeRow = writeOffset
          break
        case 2:
          writeRow = writeOffset + 1
          break
        case 4:
          writeRow = writeOffset + 2
          break
        case 8:
          writeRow = writeOffset + 3
          break
        case 16:
          writeRow = writeOffset + 4
          break
      }
      results[writeRow] = results[writeRow] + "${endLoad - startLoad},"
    }
  } // end for f
} // end r
results.each{println "${it}"}

