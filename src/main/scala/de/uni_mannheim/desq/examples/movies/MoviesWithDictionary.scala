package de.uni_mannheim.desq.examples.movies

import de.uni_mannheim.desq.dictionary._
import de.uni_mannheim.desq.mining.spark._
import org.apache.spark.{SparkConf, SparkContext}

object MoviesWithDictionary {

  def main(args: Array[String]) {
    val datasetFname = args(0)

    val patternExpression = args(1)

    val minimumSupport = args(2).toInt

    implicit val sc = new SparkContext(new SparkConf().setAppName(getClass.getName).setMaster("local"))

    // read the dictionary
    val dictionary = Dictionary.loadFrom("data/readme/dictionary.json")

    // read the data and convert it into DESQ's internal format (DesqDataset)
    val data = DesqDataset.loadFromDelFile("data/readme/sequences.del", dictionary).recomputeDictionary()

    // create a Miner
    val properties = DesqCount.createConf(patternExpression, minimumSupport)
    val miner = DesqMiner.create(new DesqMinerContext(properties))

    // do the mining; this creates another DesqDataset containing the result
    val patterns = miner.mine(data)

    // print the result
    patterns.print()
  }

}
