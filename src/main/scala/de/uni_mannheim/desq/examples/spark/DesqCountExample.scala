package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.mining.spark._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rgemulla on 14.09.2016.
  */
object DesqCountExample {
  def icdm16()(implicit sc: SparkContext) {
    val patternExpression = "[c|d]([A^|B=^]+)e"
    //val patternExpression = "(.)"
    val sigma = 2
    val conf = DesqCount.createConf(patternExpression, sigma)
    ExampleUtils.runIcdm16(conf)
  }

  def nyt()(implicit sc: SparkContext) {
    val patternExpression = "(.^ JJ@ NN@)"
    val sigma = 1000
    val conf = DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    ExampleUtils.runNyt(conf)
  }

  def exampleWithStringArrayAndLongInterpreter()(implicit sc: SparkContext) {
    val raw = sc.parallelize(Array("Anna lives in Melbourne",
      "Anna likes Bob",
      "Bob lives in Berlin",
      "Cathy loves Paris",
      "Cathy likes Dave",
      "Dave loves London",
      "Eddie lives in New York City"))

    // build Dictionary

    val dictionary = GenericDesqDataset.buildDictionaryFromStrings(raw.map(s => s.split("\\s+")))

    // create StringArrayAndLongInterpreter which holds the Dictionary

    val sequenceInterpreter = new StringArrayAndLongInterpreter()
    sequenceInterpreter.setDictionary(dictionary)

    // sequences as (Array[String], Long)

    val sequences = raw.map(s => (s.split("\\s+"), 1L))

    // mining with GenericDesqDataset[(Array[String], Long)]

    val data = new GenericDesqDataset[(Array[String], Long)](sequences, sequenceInterpreter)

    val patternExpression = "(..)"
    val minimumSupport = 2
    val properties = DesqCount.createConf(patternExpression, minimumSupport)
    val miner = DesqMiner.create(new DesqMinerContext(properties))

    val patterns = miner.mine(data)
    patterns.print()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc = new SparkContext(conf)
    //sc.setLogLevel("INFO")
    //icdm16
    //nyt
    exampleWithStringArrayAndLongInterpreter
  }
}
