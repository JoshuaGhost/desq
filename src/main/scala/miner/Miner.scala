package miner

import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}

object Miner {

  def main(args: Array[String]): Unit = {
    val datasetFname = args(0)

    val patternExpression = args(1)

    val minimumSupport = args(2).toInt

    // read the data
    implicit val sc: SparkContext = new SparkContext(new SparkConf().setAppName(getClass.getName).setMaster("local"))
    // val sequences = sc.textFile("data-local/partial_movie_docs/sequences.txt")
    val sequences = sc.textFile(datasetFname)

    // convert data into DESQ's internal format (DesqDataset)
    val data = DesqDataset.buildFromStrings(sequences.map(s => s.split("\\s+")))

    // create a Miner
    val properties = DesqCount.createConf(patternExpression, minimumSupport)
    val miner = DesqMiner.create(new DesqMinerContext(properties))

    // do the mining; this creates another DesqDataset containing the result
    val patterns = miner.mine(data)

    // print the result
    patterns.print()
  }

}
