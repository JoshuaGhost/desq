package de.uni_mannheim.desq.mining.spark

import java.net.URI
import java.util.Calendar
import java.util.zip.GZIPOutputStream

import de.uni_mannheim.desq.avro.AvroDesqDatasetDescriptor
import de.uni_mannheim.desq.dictionary.{DefaultDictionaryBuilder, DefaultSequenceBuilder, Dictionary, DictionaryBuilder}
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints._
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class GenericDesqDataset[T](val sequences: RDD[T], val descriptor: DesqDescriptor[T]) {
  private var descriptorBroadcast: Broadcast[DesqDescriptor[T]] = _

  // -- building ------------------------------------------------------------------------------------------------------

  def this(sequences: RDD[T], source: GenericDesqDataset[T]) {
    this(sequences, source.descriptor)
    descriptorBroadcast = source.descriptorBroadcast
  }

  /** Creates a copy of this GenericDesqDataset with a deep copy of its dictionary. Useful when changes should be
    * performed to a dictionary that has been broadcasted before (and hence cannot/should not be changed). */
  def copy(): GenericDesqDataset[T] = {
    new GenericDesqDataset(sequences, descriptor.copy())
  }

  /** Returns a copy of this dataset with a new dictionary, containing updated counts and fid identifiers. */
  def recomputeDictionary(): GenericDesqDataset[T] = {
    val newDescriptor = descriptor.copy()
    recomputeDictionary(newDescriptor.getDictionary)

    new GenericDesqDataset[T](sequences, newDescriptor)
  }

  /** in-place **/
  protected def recomputeDictionary(dictionary: Dictionary) = {
    // compute counts
    val descriptorBroadcast = broadcastDescriptor()
    val totalItemFreqs = sequences.mapPartitions(rows => {
      new Iterator[(Int, (Long,Long))] {
        val descriptor = descriptorBroadcast.value
        val itemCfreqs = new Int2LongOpenHashMap()
        var currentItemCfreqsIterator = itemCfreqs.int2LongEntrySet().fastIterator()
        var currentWeight = 0L
        val ancItems = new IntAVLTreeSet()

        override def hasNext: Boolean = {
          while (!currentItemCfreqsIterator.hasNext && rows.hasNext) {
            val sequence = rows.next()
            currentWeight = descriptor.getWeight(sequence)
            descriptor.getDictionary.computeItemCfreqs(descriptor.getFids(sequence), itemCfreqs, ancItems, true, 1)
            currentItemCfreqsIterator = itemCfreqs.int2LongEntrySet().fastIterator()
          }
          currentItemCfreqsIterator.hasNext
        }

        override def next(): (Int, (Long, Long)) = {
          val entry = currentItemCfreqsIterator.next()
          (entry.getIntKey, (currentWeight, entry.getLongValue*currentWeight))
        }
      }
    }).reduceByKey((c1,c2) => (c1._1+c2._1, c1._2+c2._2)).collect

    // and put them in the dictionary
    dictionary.clearFreqs() // reset all frequencies to 0 (important for items that do not occur in totalItemFreqs)
    for (itemFreqs <- totalItemFreqs) {
      val fid = itemFreqs._1
      dictionary.setDfreqOf(fid, itemFreqs._2._1)
      dictionary.setCfreqOf(fid, itemFreqs._2._2)
    }
    dictionary.recomputeFids()
  }

  // -- conversion ----------------------------------------------------------------------------------------------------

  /** Returns a DesqDataset with sequences encoded as fids.
    */
  def toDesqDatasetWithFids(): DesqDataset = {
    val descriptorBroadcast = broadcastDescriptor()

    val newSequences = sequences.mapPartitions(rows => {
      new Iterator[WeightedSequence] {
        val descriptor = descriptorBroadcast.value

        override def hasNext: Boolean = rows.hasNext

        override def next(): WeightedSequence = {
          val sequence = rows.next()
          new WeightedSequence(descriptor.getFids(sequence), descriptor.getWeight(sequence))
        }
      }
    })

    val newDescriptor = new WeightedSequenceDescriptor(usesFids = true)
    newDescriptor.setDictionary(descriptor.getDictionary)

    new DesqDataset(newSequences, newDescriptor)
  }

  /** Returns a DesqDataset with sequences encoded as fids.
    */
  def toDesqDatasetWithGids(): DesqDataset = {
    val descriptorBroadcast = broadcastDescriptor()

    val newSequences = sequences.mapPartitions(rows => {
      new Iterator[WeightedSequence] {
        val descriptor = descriptorBroadcast.value

        override def hasNext: Boolean = rows.hasNext

        override def next(): WeightedSequence = {
          val sequence = rows.next()
          new WeightedSequence(descriptor.getGids(sequence), descriptor.getWeight(sequence))
        }
      }
    })

    val newDescriptor = new WeightedSequenceDescriptor(usesFids = false)
    newDescriptor.setDictionary(descriptor.getDictionary)

    new DesqDataset(newSequences, newDescriptor)
  }

  /** Returns an RDD that contains for each sequence an array of its string identifiers and its weight. */
  def toSidsWeightPairs(): RDD[(Array[String],Long)] = {
    val descriptorBroadcast = broadcastDescriptor()

    sequences.mapPartitions(rows => {
      new Iterator[(Array[String],Long)] {
        val descriptor = descriptorBroadcast.value

        override def hasNext: Boolean = rows.hasNext

        override def next(): (Array[String], Long) = {
          val s = rows.next()
          (descriptor.getSids(s), descriptor.getWeight(s))
        }
      }
    })
  }

  // -- I/O -----------------------------------------------------------------------------------------------------------

  def save(outputPath: String)(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    val fileSystem = FileSystem.get(new URI(outputPath), sequences.context.hadoopConfiguration)

    // write sequences
    val sequencePath = s"$outputPath/sequences"
    sequences.map(s => (NullWritable.get(),descriptor.getWritable(s))).saveAsSequenceFile(sequencePath)

    // write dictionary
    val dictPath = s"$outputPath/dict.avro.gz"
    val dictOut = FileSystem.create(fileSystem, new Path(dictPath), FsPermission.getFileDefault)
    descriptor.getDictionary.writeAvro(new GZIPOutputStream(dictOut))
    dictOut.close()

    // write descriptor
    val avroDescriptor = new AvroDesqDatasetDescriptor()
    avroDescriptor.setCreationTime(Calendar.getInstance().getTime.toString)
    val avroDescriptorPath = s"$outputPath/descriptor.json"
    val avroDescriptorOut = FileSystem.create(fileSystem, new Path(avroDescriptorPath), FsPermission.getFileDefault)
    val writer = new SpecificDatumWriter[AvroDesqDatasetDescriptor](classOf[AvroDesqDatasetDescriptor])
    val encoder = EncoderFactory.get.jsonEncoder(avroDescriptor.getSchema, avroDescriptorOut)
    writer.write(avroDescriptor, encoder)
    encoder.flush()
    avroDescriptorOut.close()

    // return a new dataset for the just saved data
    new GenericDesqDataset[T](
      sequences.context.sequenceFile(sequencePath, classOf[NullWritable], classOf[Writable]).map(kv => kv._2).asInstanceOf[RDD[T]],
      descriptor)
  }


  // -- mining --------------------------------------------------------------------------------------------------------

  def mine(minerConf: DesqProperties)(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    val ctx = new DesqMinerContext(minerConf)
    mine(ctx)
  }

  def mine(ctx: DesqMinerContext)(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    val miner = DesqMiner.create(ctx)
    mine(miner)
  }

  def mine(miner: DesqMiner)(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    miner.mine(this)
  }


  // -- helpers -------------------------------------------------------------------------------------------------------

  /** Returns a broadcast variable that can be used to access the descriptor of this dataset. The broadcast
    * variable stores the dictionary contained in the descriptor in serialized form for memory efficiency.
    * Use <code>Dictionary.fromBytes(result.value.getDictionary)</code> to get the dictionary at workers.
    */
  def broadcastDescriptor(): Broadcast[DesqDescriptor[T]] = {
    if (descriptorBroadcast == null) {
      val descriptor = this.descriptor
      descriptorBroadcast = sequences.context.broadcast(descriptor)
    }
    descriptorBroadcast
  }

  /** Pretty prints up to <code>maxSequences</code> sequences contained in this dataset using their sid's. */
  def print(maxSequences: Int = -1): Unit = {
    val strings = toSidsWeightPairs().map(s => {
      val sidString = s._1.deep.mkString("[", " ", "]")
      if (s._2 == 1)
        sidString
      else
        sidString + "@" + s._2
    })
    if (maxSequences < 0)
      strings.collect().foreach(println)
    else
      strings.take(maxSequences).foreach(println)
  }
}

object GenericDesqDataset {

  // -- building ------------------------------------------------------------------------------------------------------

  /** Builds a GenericDesqDataset from an RDD of string arrays. Every array corresponds to one sequence, every element
    * to one item. The generated hierarchy is flat. */
  def buildFromStrings[T](rawData: RDD[Array[String]], descriptor: DesqDescriptor[T])(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    val parse = (strings: Array[String], seqBuilder: DictionaryBuilder) => {
      seqBuilder.newSequence(1)
      for (string <- strings) {
        seqBuilder.appendItem(string)
      }
    }

    build[Array[String], T](rawData, parse, descriptor)
  }

  /** Builds a GenericDesqDataset from arbitrary input data. The dataset is linked to the original data and parses
    * it again when used. For improved performance, save the dataset once created.
    *
    * @param rawData the input data as an RDD
    * @param parse method that takes an input element, parses it, and registers the resulting items (and their parents)
    *              with the provided DictionaryBuilder. Used to construct the dictionary and to translate the data.
    * @param descriptor the DesqDescriptor that should be used
    * @tparam R type of input data elements
    * @tparam T type of output GenericDesqDataset
    * @return the created GenericDesqDataset
    */
  def build[R, T](rawData: RDD[R], parse: (R, DictionaryBuilder) => _, descriptor: DesqDescriptor[T])(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    val dict = rawData.mapPartitions(rows => {
      val dictBuilder = new DefaultDictionaryBuilder()
      while (rows.hasNext) {
        parse.apply(rows.next(), dictBuilder)
      }
      dictBuilder.newSequence(0) // flush last sequence
      Iterator.single(dictBuilder.getDictionary)
    }).treeReduce((d1, d2) => {
      d1.mergeWith(d2); d1
    }, 3)
    dict.recomputeFids()

    descriptor.setDictionary(dict)

    // now convert the sequences (lazily)
    val descriptorBroadcast = rawData.context.broadcast(descriptor)
    val sequences = rawData.mapPartitions(rows => new Iterator[T] {
      val descriptor = descriptorBroadcast.value
      val seqBuilder = new DefaultSequenceBuilder(dict)

      override def hasNext: Boolean = rows.hasNext

      override def next(): T = {
        parse.apply(rows.next(), seqBuilder)
        descriptor.construct().apply(seqBuilder.getCurrentGids, seqBuilder.getCurrentWeight)
      }
    })

    val result = new GenericDesqDataset[T](sequences, descriptor)
    result
  }

}
