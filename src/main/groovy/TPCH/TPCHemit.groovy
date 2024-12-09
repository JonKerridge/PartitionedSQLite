package TPCH

import cluster_cli.records.EmitInterface

class TPCHemit implements  EmitInterface<TPCHemit>, Serializable{
  // properties of basic constructor
  int nodes, workers
  String specFileName //includes full path

  // properties of the class obtained from spec file
  List <List> buildData  //store the spec file content
  int nPragmaStatements
  int nTableCreates
  int nIndexBuilds
  String dataFileDelimiter
  String databaseRootURL
  List <String> pragmas
  int cts  //current table subscript in buildData
  int nEntries, lastTableEntry

  // properties used in constructors
  String component, s1, s2, s3, s4
  List types
  int partitions, partitionSize



  TPCHemit(List d){
    nodes = (int) d[0]
    workers = (int)d[1]
    specFileName = d[2]
    // read the specFile
    List <List> buildData
    buildData = []
    File specFile = new File(specFileName)
    if (!specFile.exists())
      println "spec file ${specFileName} does not exist"
    else {
      specFile.withObjectInputStream {inStream ->
        inStream.eachObject {buildData << it} }
    }
    nPragmaStatements = buildData[0][0]
    nTableCreates = buildData[0][1]
    nIndexBuilds =  buildData[0][2]
    dataFileDelimiter = buildData[0][3]
    databaseRootURL = buildData[0][4]
    pragmas = []
    for ( i in 0 ..< nPragmaStatements) pragmas << buildData[1][i]
    cts = 2
    nEntries = buildData.size()
    lastTableEntry = 1 + nTableCreates  // subscript of last table create entry
  } //TPCHemit

  TPCHemit (String component, s1, s2, s3, s4, List types, int partitions, partitionSize) {

  }// TPCHemit

  TPCHemit (String component, s1, s2){
    this.component = component
    this.s1 = s1
    this.s2 = s2
  }
  

  @Override
  TPCHemit create() {
    if (cts == nEntries) return null  //database building complete
    if (cts > lastTableEntry) {
      // building indexes
      TPCHemit tpcIndex = new TPCHemit('index', buildData[cts][0], buildData[cts][1])
      // more data
      cts++
      return tpcIndex
    }
    else {
      // importing data into tables
      TPCHemit tpcTable = new TPCHemit('table')
      // more data
      cts++
      return tpcTable
    }
  } // create
}
