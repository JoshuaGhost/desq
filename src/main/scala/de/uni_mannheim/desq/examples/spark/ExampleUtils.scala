package de.uni_mannheim.desq.examples.spark

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner, DesqMinerContext}
import de.uni_mannheim.desq.util.DesqProperties
import org.apache.spark.SparkContext

import scala.io.Source

/**
  * Created by rgemulla on 14.09.2016.
  */
object ExampleUtils {
  /**
    * Creates a miner, runs it on the given data, and prints running times as well as pattern statistics.
    * Result sequences are cached in memory.
    */
  def runMiner(data: DesqDataset[WeightedSequence], ctx: DesqMinerContext): (DesqMiner, DesqDataset[WeightedSequence]) = {
    println("Miner properties: ")
    ctx.conf.prettyPrint()

    print("Creating miner... ")
    val prepTime = Stopwatch.createStarted
    val miner = DesqMiner.create(ctx)
    prepTime.stop
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Mining (RDD construction)... ")
    val miningTime = Stopwatch.createStarted
    val result = miner.mine(data)
    miningTime.stop
    println(miningTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Mining (persist)... ")
    val persistTime = Stopwatch.createStarted
    result.sequences.cache()
    val (count, support) = result.sequences.map(ws => (1, ws.weight)).reduce((cs1, cs2) => (cs1._1+cs2._1, cs1._2+cs2._2))
    persistTime.stop
    println(persistTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    println("Total time: " + (prepTime.elapsed(TimeUnit.MILLISECONDS)
      + miningTime.elapsed(TimeUnit.MILLISECONDS) + persistTime.elapsed(TimeUnit.MILLISECONDS)) + "ms")

    println("Number of patterns: " + count)
    println("Total frequency of all patterns: " + support)

    (miner, result)
  }

  /**
    * Creates a miner, runs it on the given data, and prints running times as well as pattern statistics.
    * Result sequences are cached in memory.
    */
  @throws[IOException]
  def runMiner(data: DesqDataset[WeightedSequence], minerConf: DesqProperties): (DesqMiner, DesqDataset[WeightedSequence]) = {
    val ctx = new DesqMinerContext(minerConf)
    runMiner(data, ctx)
  }

  /**
    * Creates a miner, runs it on the given data, and prints running times as well as all mined patterns.
    */
  def runVerbose(data: DesqDataset[WeightedSequence], minerConf: DesqProperties): (DesqMiner, DesqDataset[WeightedSequence]) = {
    val (miner, result) = runMiner(data, minerConf)

    System.out.println("\nPatterns:")
    result.print()

    (miner, result)
  }

  /** Runs a miner on ICDM16 example data. */
  @throws[IOException]
  def runIcdm16(minerConf: DesqProperties)(implicit sc: SparkContext): (DesqMiner, DesqDataset[WeightedSequence]) = {
    val dictFile = this.getClass.getResource("/icdm16-example/dict.json")
    val dataFile = this.getClass.getResource("/icdm16-example/data.del")

    // load the dictionary & update hierarchy
    val dict = Dictionary.loadFrom(dictFile)
    val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
    val data = DesqDataset.loadFromDelFile(delFile, dict, usesFids = false).copyWithRecomputedCountsAndFids()
    println("\nDictionary with frequencies:")
    dict.writeJson(System.out)
    println()

    // print sequences
    System.out.println("\nInput sequences:")
    data.print()
    println()

    val (miner, result) = runVerbose(data, minerConf)
    result.sequences.unpersist()
    (miner, result)
  }

  /** Runs a miner on NYT example data. */
  @throws[IOException]
  def runNyt(minerConf: DesqProperties, verbose: Boolean = false)(implicit sc: SparkContext): (DesqMiner, DesqDataset[WeightedSequence]) = {
    val dict: Dictionary = Dictionary.loadFrom("data-local/nyt-1991-dict.avro.gz")
    val delFilename = "data-local/nyt-1991-data.del"
    val delFile = sc.textFile(delFilename)
    val data = DesqDataset.loadFromDelFile(delFile, dict, usesFids = true)
    if (verbose)
      runMiner(data, minerConf)
    else
      runVerbose(data, minerConf)
  }
}
