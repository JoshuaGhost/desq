package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq
import de.uni_mannheim.desq.mining.spark.DesqCount
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.{Logger, LogManager}
import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Other way to run DesqDfs
  * Created by alexrenz on 08.10.2016.
  */
object RunStuff {
  
  def main(args: Array[String]) {
    if(args.length < 4) {
      println("Usage: RunStuff inputPath outputPath minSupport patternExpression")
      System.exit(0)
    }
    val logger = LogManager.getLogger("DesqRunner")
    
    // Init Desq, build SparkContext
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    Desq.initDesq(sparkConf)
    implicit val sc = new SparkContext(sparkConf)
      
    val path = args(0)
    val data = DesqDataset.load(path)
    
    //println("\n\nDictionary with frequencies:")
    //data.dict.writeJson(System.out)
    
    //println("\n\nInput sequences as letters:")
    //data.print()
    
    val sigma = args(2).toInt
    val patternExpression = args(3)
    
    // Build miner conf
    val minerConf = DesqCount.createConf(patternExpression, sigma)
    
    minerConf.setProperty("desq.mining.prune.irrelevant.inputs", "true")
    minerConf.setProperty("desq.mining.use.two.pass", "false")
    minerConf.setProperty("desq.mining.use.flist", "true")
    
    // Build miner
    val ctx = new DesqMinerContext(minerConf)
    println("Miner properties: ")
    ctx.conf.prettyPrint()
    val miner = DesqMiner.create(ctx)
    
    // Mine
    val result = miner.mine(data)
    result.sequences.saveAsTextFile(args(1))
  }
}

