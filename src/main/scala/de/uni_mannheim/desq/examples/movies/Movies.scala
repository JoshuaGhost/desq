package de.uni_mannheim.desq.examples.movies

import de.uni_mannheim.desq.mining.spark._
import org.apache.spark.{SparkConf, SparkContext}

object Movies {

  def main(args: Array[String]) {
    val datasetFname = args(0)
    val datasetPath = datasetFname.slice(0, datasetFname.lastIndexOf("/"))
    val tempFname = datasetPath + "/sequences.del"

    val patternExpression = args(1)

    val minimumSupport = args(2).toInt

    implicit val sc = new SparkContext(new SparkConf().setAppName(getClass.getName).setMaster("local"))

    // read the data
    // val sequences = sc.textFile("data-local/partial_movie_docs/sequences.txt")
    val sequences = sc.textFile(datasetPath)

    // convert data into DESQ's internal format (DesqDataset)
    val data = DesqDataset.buildFromStrings(sequences.map(s => s.split("\\s+")))

    data.save(tempFname)

    // create a Miner
    // val patternExpression = "(....)"
    val properties = DesqCount.createConf(patternExpression, minimumSupport)
    val miner = DesqMiner.create(new DesqMinerContext(properties))

    // do the mining; this creates another DesqDataset containing the result
    val patterns = miner.mine(data)

    // print the result
    patterns.print()
  }

}
