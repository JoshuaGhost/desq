package de.uni_mannheim.desq.mining.spark

import java.util.Collections

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList, IntSet, IntIterator, IntArraySet}
import it.unimi.dsi.fastutil.objects.{ObjectIterator, ObjectLists}
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import scala.collection.JavaConverters._

/**
  * Created by alexrenz on 05.10.2016.
  */
class DesqDfs(ctx: DesqMinerContext) extends DesqMiner(ctx) {
  override def mine(data: DesqDataset): DesqDataset = {
    // localize the variables we need in the RDD
    val serializedDict = data.broadcastSerializedDictionary()
    val conf = ctx.conf
    val usesFids = data.usesFids
    val minSupport = conf.getLong("desq.mining.min.support")

      
    
    // In a first step, we map over each input sequence and output (output item, input sequence) pairs
    //   for all possible output items in that input sequence
    // Second, we group these pairs by key [output item], so that we get an RDD like this:
    //   (output item, Iterable[input sequences])
    // Third step: see below
    val outputItemPartitions = data.sequences.mapPartitions(rows => {
      
      // for each row, determine the possible output elements from that input sequence and create 
      //   a (output item, input sequence) pair for all of the possible output items
      new Iterator[(Int, IntList)] {
        // initialize the sequential desq miner
        val dict = Dictionary.fromBytes(serializedDict.value)
        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
        baseContext.dict = dict
        baseContext.conf = conf
        val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext)
        
        var outputIterator: IntIterator = new IntArraySet(0).iterator
        var currentSupport = 0L
        var itemFids = new IntArrayList
        var currentSequence : WeightedSequence = _

        // We want to output all (output item, input sequence pairs for the current 
        //   sequence. Once we are done, we move on to the next input sequence in this partition.
        override def hasNext: Boolean = {
          // do we still have pairs to output for this sequence?
          while (!outputIterator.hasNext && rows.hasNext) {
            // if not, go to the next input sequence
            currentSequence = rows.next()
            currentSupport = currentSequence.support

            // for that new input sequence, find all possible output sequences and add them to the outputIterator
            if (usesFids) {
              outputIterator = baseMiner.getFreqOutputItemsOfOneSequence(currentSequence.items).iterator()
            } else {
              dict.gidsToFids(currentSequence.items, itemFids)
              outputIterator = baseMiner.getFreqOutputItemsOfOneSequence(itemFids).iterator()
            }
          }

          outputIterator.hasNext
        }

        // we simply output (output item, input sequence)
        override def next(): (Int, IntList) = {
          val outputItem = outputIterator.next()
          
          // There is some weird behavior if we use itemFids here. So for now, let's just emit
          //   the sequene as is and convert it later, again.
          //   Not as bad as it sounds as we will mostly use fid-data sets
          (outputItem, currentSequence.items)
        }
      }
    }).groupByKey()
    
    // Third, we flatMap over the (output item, Iterable[input sequences]) RDD to mine each partition,
    //   with respect to the pivot item (=output item) of each partition. 
    //   At each partition, we only output sequences where the respective output item is the maximum item
    val patterns = outputItemPartitions.flatMap{ (row) =>
      val partitionItem = row._1
      val sequencesIt = row._2.iterator
      
      // grab the necessary variables
      val dict = Dictionary.fromBytes(serializedDict.value)
      val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
      baseContext.dict = dict
      baseContext.conf = conf
      
      // Set a memory pattern writer so we are able to retrieve the patterns later
      // TODO: Check whether this intended like this 
      val result : MemoryPatternWriter = new MemoryPatternWriter()
      baseContext.patternWriter = result
      
      // Set up the miner
      val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext)
      
      // Add sequences to miner
      // TODO: have an option to add many sequences at once?
      var s : IntList = new IntArrayList
      var fids : IntList = new IntArrayList
      for(s <- sequencesIt) {
        if(usesFids) {
          baseMiner.addInputSequence(s, 1)
        } else {
          dict.gidsToFids(s, fids)
          baseMiner.addInputSequence(fids, 1)
        }
      }
        
      // Mine this partition, only output patterns where the output item is the maximum item
      baseMiner.minePivot(partitionItem)
              
      // TODO: find a better way to get a Java List accepted as a Scala TraversableOnce
      result.getPatterns().asScala
    }
    
    // all done, return result (last parameter is true because mining.DesqCount always produces fids)
    new DesqDataset(patterns, data, true)
  }
}

object DesqDfs {
  def createConf(patternExpression: String, sigma: Long): DesqProperties = {
    val conf = de.uni_mannheim.desq.mining.DesqDfs.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.miner.class", classOf[DesqDfs].getCanonicalName)
    conf
  }
}