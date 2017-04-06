package de.uni_mannheim.desq.converters.nyt

import java.io.File
import java.util

import com.google.common.io.Files
import de.uni_mannheim.desq.comparing.{DesqCompareMulti, DesqCountMulti}
import de.uni_mannheim.desq.converters.nyt.avroschema.{Article, Sentence, Span}
import de.uni_mannheim.desq.dictionary.DefaultDictionaryBuilder
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.spark._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by ivo on 22.03.17.
  */
class nyt(val rootDir: String) {

  val builder = new DefaultDictionaryBuilder()

  val itemFids = new IntArrayList()

  val subDirs = ConvertNyt.getLeafSubdirs(new File(rootDir))


  def readNytSpark: Unit = {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    implicit val sc = new SparkContext(conf)
    val path = "./data-local/NYTimesProcessed/results/2005"
    val subDirs = ConvertNyt.getLeafSubdirs(new File(path))
    val paths = for (folder: File <- subDirs.asScala;
                     file: File <- folder.listFiles();
                     if Files.getFileExtension(file.getPath()).endsWith("avro")
    ) yield {
      file.getPath()
    }


    val job = AvroUtils.getJobInputKeyAvroSchema(Article.SCHEMA$)
    val schema = Article.SCHEMA$
    val spark = SparkSession.builder().master("local").getOrCreate()

    val articles = paths.toList.map(path => spark.read.format("com.databricks.spark.avro").option("avroSchema", schema.toString).load(path)).reduce((df1, df2) => df1.union(df2)).cache
    //    val articles = spark.read.format("com.databricks.spark.avro").option("avroSchema", schema.toString).load(path).cache()

    import org.apache.spark.sql.functions._
    val sentences = articles.select(articles.col("filename"), explode(articles.col("sentences")))

    val tokens = sentences.select(explode(sentences.col("col.tokens.word")), articles.col("filename"))
    val index = tokens.rdd.map(row => (row.getString(0), row.getString(1)))

    val initialSet = mutable.HashSet.empty[String]
    val addTokenDocPair = (set: mutable.HashSet[String], pair: String) => set += pair
    val mergeSets = (set1: mutable.HashSet[String], set2: mutable.HashSet[String]) => set1 ++= set2

    val uniqueByKey = index.aggregateByKey(initialSet)(addTokenDocPair, mergeSets)

    val filter = uniqueByKey.filter(_._1.matches("Trump")).filter(!_._1.matches("Clinton"))
    val articlesFiltered = articles.filter(articles.col("filename").isin(filter.collect().apply(0)._2.toList: _*))

    val filterRight = uniqueByKey.filter(_._1.matches("Clinton")).filter(!_._1.matches("Trump"))
    val articlesFilteredRight = articles.filter(articles.col("filename").isin(filterRight.collect().apply(0)._2.toList: _*))

    val roomRight = new Newsroom("./data-local/processed/results/", "right")
    roomRight.initialize()
    for (article <- articlesFilteredRight.collect()) {
      val avroArticle = convertToArticle(article)
      roomRight.processArticle(avroArticle)
    }
    roomRight.shutdown()

    val room = new Newsroom("./data-local/processed/results/", "left")
    room.initialize()
    for (article <- articlesFiltered.collect()) {
      val avroArticle = convertToArticle(article)
      room.processArticle(avroArticle)
    }
    room.shutdown()

    val multi = DesqCountMulti.run()
    val compare = new DesqCompareMulti(multi)
    val leftSeq = compare.compare(compare.leftDataset, compare.rightDataset)
    val rightSeq = compare.compare(compare.leftDataset, compare.rightDataset)
    leftSeq.sequences
    println("*******************************************LEFT SEQUENCES*******************************************")
    leftSeq.print()
    println("*******************************************RIGHT SEQUENCES******************************************")
    rightSeq.print()
    //    compare.compare(compare.leftCurrent, compare.itLeft, compare.rightCurrent, compare.itRight)
  }


  def convertToArticle(row: Row): Article = {
    val article = new Article()
    article.setAbstract$(row.getString(0))
    article.setSentences(convertToSentence(row.getSeq[Any](2)))
    article
  }

  def convertToSentence(sentencesRaw: Seq[Any]): util.List[Sentence] = {
    val sentences = for (sentenceRaw <- sentencesRaw) yield {
      val sentence = new Sentence
      sentence.setTokens(convertToTokens(sentenceRaw.asInstanceOf[GenericRowWithSchema].getSeq(0)))
      sentence.setSId(sentenceRaw.asInstanceOf[GenericRowWithSchema].getInt(1))
      sentence.setSg(sentenceRaw.asInstanceOf[GenericRowWithSchema].getString(2))
      sentence.setDp(sentenceRaw.asInstanceOf[GenericRowWithSchema].getString(3))
      sentence.setSpan(convertToSpan(sentenceRaw.asInstanceOf[GenericRowWithSchema].get(4)))
      sentence
    }
    sentences.asJava
  }

  def convertToTokens(tokensRaw: Seq[Any]): util.List[avroschema.Token] = {
    val tokens = for (tokenRaw <- tokensRaw) yield {
      val token = new avroschema.Token()
      token.setPos(tokenRaw.asInstanceOf[GenericRowWithSchema].getString(0))
      token.setNer(tokenRaw.asInstanceOf[GenericRowWithSchema].getString(1))
      token.setSpan(convertToSpan(tokenRaw.asInstanceOf[GenericRowWithSchema].get(2)))
      token.setLemma(tokenRaw.asInstanceOf[GenericRowWithSchema].getString(3))
      token.setWord(tokenRaw.asInstanceOf[GenericRowWithSchema].getString(4))
      token.setIndex(tokenRaw.asInstanceOf[GenericRowWithSchema].getInt(5))
      token
    }
    tokens.asJava
  }

  def convertToSpan(rawSpan: Any): Span = {
    val span = new Span()
    span.setStartIndex(rawSpan.asInstanceOf[GenericRowWithSchema].getInt(0))
    span.setEndIndex(rawSpan.asInstanceOf[GenericRowWithSchema].getInt(1))
    span
  }
}

object Converter extends App {
  val converter = new nyt("data-local/NYTimesProcessed/results/2006")
  converter.readNytSpark

}

