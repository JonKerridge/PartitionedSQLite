package TPCH

import cluster_cli.records.EmitInterface

class TPCHemit implements  EmitInterface<TPCHemit>, Serializable{
  // properties of basic constructor
  int nodes, workers
  String specFileName //includes full path of commands.spec file

  // properties of the class obtained from spec file
  List <List> buildData  //store the spec file content
  int nPragmaStatements
  int nTableCreates
//  int nIndexBuilds not SQLite
  String dataFileDelimiter
  String databaseRootURL
  List <String> pragmas
  int cts  //current table subscript in buildData
  int nEntries

  // properties used in constructors
  String component, s1, s2, s3, s4
  List types
//  int partitions, partitionSize not SQLite



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
//    nIndexBuilds =  buildData[0][2] not SQLite see subsequent changes
    dataFileDelimiter = buildData[0][2]
    databaseRootURL = buildData[0][3]
    pragmas = []
    for ( i in 0 ..< nPragmaStatements) pragmas << buildData[1][i]
    cts = 2
    nEntries = buildData.size()
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
    else {
      // importing data into tables
      TPCHemit tpcTable = new TPCHemit('table')
      // more data
      cts++
      return tpcTable
    }
  } // create
}
