package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDfs}
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import java.text.SimpleDateFormat

import org.apache.log4j.{LogManager, Logger}
import java.util.Date


/**
  * Other way to run DesqDfs
  * Created by alexrenz on 08.10.2016.
  */
object DesqRunner {
  var sparkConf : SparkConf = _
  implicit var sc: SparkContext = _

  var sigma: Long = _
  var patternExp: String = _
  var dataFile: File = _
  var runVersion: String = _
  var verbose: Boolean = _
  var scenarioStr: String = _
  var useCase: String = _
  var scenario: Int = _
  var baseFolder: String = _
  var dataDir: String = _

  // Switches
  var sendNFAs: Boolean = _
  var mergeSuffixes: Boolean = _
  var useDesqCount: Boolean = _
  var useTwoPass: Boolean = _
  var maxNumberShuffleOutputItems: Integer = _

  val runConf = scala.collection.mutable.Map[String,String]()


  def main(args: Array[String]) {

    // defaults
    if(args.length == 0) {
      runConf.put("master", "local[1]")
    }

    runConf.put("map.repartition", "0")

    if(args.length > 0) {
      // parse args
      for(arg <- args) {
        val splits = arg.split("=")
        runConf.put(splits(0),{if(splits.length > 1) splits(1) else ""})
      }
    }

    println(runConf)

    var appName = getClass.getName
    if(runConf.contains("case"))
      appName = runConf.get("case").get + " s" + runConf.get("scenario").get + " r" + runConf.get("run").get
    // Init Desq, build SparkContext
    if(!runConf.contains("master")) {
      sparkConf = new SparkConf().setAppName(appName)
    }
    else {
      sparkConf = new SparkConf().setAppName(appName).setMaster(runConf.get("master").get)
    }

    Desq.initDesq(sparkConf)
    sc = new SparkContext(sparkConf)

    if(args.length > 0) {
      setDataLoc(runConf.get("loc").get)
      runDesq()
    } else {
      setDataLoc("")
//      prepDataset(); System.exit(0)
      runGrid(); System.exit(0)
      runDesq("I2", 2, 1)
    }
  }

  def runGrid() {
    val tests = Array("I1@1", "I1@2", "I2", "IA2", "IA4", "IX1", "IX2", "IX3", "IX4")
//    val scenarios = Array(0, 1, 2, 3, 4, 5, 6)
    val scenarios = Array(0, 2)

    var output = ""
    for (testCase <- tests) {
      for (scenario <- scenarios) {
        val res = runDesq(testCase, scenario, 1)
        output += testCase + " // " + scenario + " // \t" + res._1 + "\t" + res._2 + "\n"
      }
      output += "\n"
    }

    System.out.println("###############################################################")
    System.out.println("###############################################################")
    System.out.println(output)
  }

  def prepDataset(): Unit = {

    var dict = Dictionary.loadFrom(baseFolder.substring(7) + "Data/icdm16fids/dict.json")
    var data = DesqDataset.loadFromDelFile(baseFolder.substring(7) + "Data/icdm16fids/data.del", dict, true)
    data.save(baseFolder + "/Data/prep/icdm16fids")

    var ds = "nyt-1991"
    dict = Dictionary.loadFrom(baseFolder.substring(7) + "Data/" + ds + "/" + ds + "-dict.json")
    data = DesqDataset.loadFromDelFile(baseFolder.substring(7) + "Data/" + ds + "/" + ds + "-data.del", dict, true)
    data.save(baseFolder + "/Data/prep/" + ds)

    ds = "nyt"
    dict = Dictionary.loadFrom(baseFolder.substring(7) + "Data/" + ds + "/" + ds + "-dict.json")
    data = DesqDataset.loadFromDelFile(baseFolder.substring(7) + "Data/" + ds + "/" + ds + "-data.del", dict, true)
    data.save(baseFolder + "/Data/prep/" + ds)

    ds = "amzn"
    dict = Dictionary.loadFrom(baseFolder.substring(7) + "Data/" + ds + "/" + ds + "-dict.json")
    data = DesqDataset.loadFromDelFile(baseFolder.substring(7) + "Data/" + ds + "/" + ds + "-data.del", dict, true)
    data.save(baseFolder + "/Data/prep/" + ds)
  }

  def runDesq(theCase: String, scenario: Int, run: Int) : (Long, Long) = {
    runConf.put("case", theCase)
    runConf.put("scenario", scenario.toString)
    runConf.put("run", run.toString)

    runDesq()
  }

  def runDesq() : (Long, Long) = {

    setCase(runConf.get("case").get)
    setScenario(runConf.get("scenario").get.toInt)

    val logger = LogManager.getLogger("DesqRunner")

    System.out.println("------------------------------------------------------------------")
    System.out.println("Distributed Mining " + runConf.get("case").get + " @ " + scenarioStr + "  #" + runConf.get("run").get)
    System.out.println("------------------------------------------------------------------")

    println(sparkConf.toDebugString)

    println("Load dataset from " + dataDir)
    val data = DesqDataset.load(dataDir)
    
    // Build miner conf
    var minerConf = DesqDfs.createConf(patternExp, sigma)
    if(useDesqCount) {
      minerConf = DesqCount.createConf(patternExp, sigma)
    }
    minerConf.setProperty("desq.mining.prune.irrelevant.inputs", "false")
    minerConf.setProperty("desq.mining.use.two.pass", useTwoPass)
    minerConf.setProperty("desq.mining.send.nfas", sendNFAs)
    minerConf.setProperty("desq.mining.merge.suffixes", mergeSuffixes)
    minerConf.setProperty("desq.mining.shuffle.max.num.output.items", maxNumberShuffleOutputItems)

    minerConf.setProperty("desq.mining.map.repartition", runConf.get("map.repartition").get)

    if(runConf.contains("dict.from.hdfs"))
      minerConf.setProperty("desq.mining.spark.dict.hdfs", dataDir)

    // Build miner
    val ctx = new DesqMinerContext(minerConf)
    println("Miner properties: ")
    ctx.conf.prettyPrint()
    val miner = DesqMiner.create(ctx)
    
    // Mine
    var t1 = System.nanoTime // we are not using the Guava stopwatch here due to the packaging conflicts inside Spark (Guava 14)
    print("Mining (RDD construction)... ")
    val result = miner.mine(data)
    val date = new Date()
    val sdf = new SimpleDateFormat("HHmmss")
    // for now, let's simply count (below)
    //result.sequences.cache().saveAsTextFile(baseFolder + "Output/" + useCase + "-" + scenario + "-" + run + "-" + sdf.format(date))
    var freq = 0L
    val count = result.sequences.cache().count()
    println("Pattern count: " + count)
    if (count > 0) {
      freq = result.sequences.map(_.weight).sum().toLong
      println("Pattern freq:  " + freq)
    }
    val mineAndOutputTime = (System.nanoTime - t1) / 1e9d
    logger.fatal("mineAndOutputTime: " + mineAndOutputTime + "s")

    println("count;freq;"+count+";"+freq)

    (count, freq)
  }


  def setCase(thisUseCase: String) {
    verbose = false
    useCase = thisUseCase
    useCase match {
      case "N1-1991" | "N1" => {
        patternExp = "ENTITY@ (VB@+ NN@+? IN@?) ENTITY@"
        sigma = 10
        if (useCase.contains("1991")) sigma = sigma / 10
        setNytData()
      }
      case "N2-1991" | "N2" => {
        patternExp = "(ENTITY@^ VB@+ NN@+? IN@? ENTITY@^)"
        sigma = 100
        if (useCase.contains("1991")) sigma = sigma / 10
        setNytData()
      }
      case "N3-1991" | "N3" => {
        patternExp = "(ENTITY@^ be@VB@=^) DT@? (RB@? JJ@? NN@)"
        sigma = 10
        if (useCase.contains("1991")) sigma = sigma / 10
        setNytData()
      }
      case "N4-1991" | "N4" => {
        patternExp = "(.^){3} NN@"
        sigma = 1000
        if (useCase.contains("1991")) sigma = sigma / 10
        setNytData()
      }
      case "N5-1991" | "N5" => {
        patternExp = "([.^ . .]|[. .^ .]|[. . .^])"
        sigma = 1000
        if (useCase.contains("1991")) sigma = sigma / 10
        setNytData()
      }
      case "A1" => {
        patternExp = "(Electronics^)[.{0,2}(Electronics^)]{1,4}"
        sigma = 500
        setAmznData()
      }
      case "A2" => {
        patternExp = "(Books)[.{0,2}(Books)]{1,4}"
        sigma = 100
        setAmznData()
      }
      case "A3" => {
        patternExp = "Digital_Cameras@Electronics[.{0,3}(.^)]{1,4}"
        sigma = 100
        setAmznData()
      }
      case "A4" => {
        patternExp = "(Musical_Instruments^)[.{0,2}(Musical_Instruments^)]{1,4}"
        sigma = 100
        setAmznData()
      }
      case "I1@1" => {
        patternExp = "[c|d]([A^|B=^]+)e"
        sigma = 1
        verbose = true
        setICDMData()
      }
      case "I1@2" => {
        patternExp = "[c|d]([A^|B=^]+)e"
        sigma = 2
        verbose = true
        setICDMData()
      }
      case "I2" => {
        patternExp = "([.^ . .])"
        sigma = 1
        verbose = true
        setICDMData()
      }
      case "IA2" => {
        patternExp = "(A)[.{0,2}(A)]{1,4}"
        sigma = 1
        verbose = true
        setICDMData()
      }
      case "IA4" => {
        patternExp = "(A^)[.{0,2}(A^)]{1,4}"
        sigma = 1
        verbose = true
        setICDMData()
      }
      case "IX1" => {
        patternExp = "[c|d](a2).*([A^|B=^]).*(e)"
        sigma = 1
        verbose = true
        setICDMData()
      }
      case "IX2" => {
        patternExp = "[c|d](a2).*([A^|B=^]).*(B^e)"
        sigma = 1
        verbose = true
        setICDMData()
      }
      case "IX3" => {
        patternExp = "(a1* b12 e)"
        sigma = 1
        verbose = true
        setICDMData()
      }
      case "IX4" => {
        patternExp = "([c|a1] .* [.* A]+ .* [d|e])"
        sigma = 1
        verbose = true
        setICDMData()
      }
      case _ => {
        System.out.println("Do not know the use case " + useCase)
        System.exit(1)
      }
    }
  }

  def setScenario(setScenario: Int) {
    //set some defaults
    scenario = setScenario
    sendNFAs = false
    mergeSuffixes = false
    useDesqCount = false
    useTwoPass = false
    maxNumberShuffleOutputItems = 2
    scenario match {
      case 0 =>
        scenarioStr = "Count, shuffle output sequences"
        useDesqCount = true
        useTwoPass = true
      case 1 =>
        scenarioStr = "Dfs, shuffle input sequences"
        useTwoPass = true
      case 2 =>
        scenarioStr = "Dfs, shuffle transition DAGs, two-pass, generalize inputs"
        sendNFAs = true
        mergeSuffixes = true
        useTwoPass = true
      case _ =>
        System.out.println("Unknown variant")
        System.exit(0)
    }
  }

  def setDataLoc(loc: String) {
    if(loc.startsWith("hdfs")) {
      baseFolder = "hdfs:///user/alex/"
    } else {
      if(System.getProperty("os.name").startsWith("Mac")) {
        baseFolder = "file:///Users/alex/"
      } else {
        baseFolder = "file:///home/alex/"
      }
    }
  }

  def setAmznData() {
    dataDir = baseFolder + "Data/prep/amzn/"
  }

  def setICDMData() {
    dataDir = baseFolder + "Data/prep/icdm16fids/"
  }

  def setNytData() {
    var dataset = "nyt"
    if(useCase.contains("1991")) {
      dataset = "nyt-1991"
    }
    dataDir = baseFolder + "Data/prep/" + dataset + "/"
  }
}
