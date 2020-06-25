package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import scala.collection.mutable.Map

class ConfPairsPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold of co-occurrence", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairsPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold of co-occurrence: " + args.threshold());

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val MAX_WORD = 40
    val threshold = args.threshold()
    val lineNum = textFile.count()
    val wordCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(MAX_WORD).toSet
        tokens.map(p => (p, 1)).toList
      })
      .reduceByKey(_ + _)
      .collectAsMap()
    val wordList = sc.broadcast(wordCounts);

    val result = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(MAX_WORD).distinct
        if (tokens.length > 1) {
          val pairsList = tokens.combinations(2)
          val Forward = pairsList.map{ case Seq(x, y) => (x, y) }.toList
          val Backward = Forward.map(p => (p._2, p._1)).toList
          Forward ++ Backward
        } else List()
      })
      .map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .filter(p => (p._2 >= threshold))
      .map(p => {
        val left = wordList.value(p._1._1).toDouble
        val right = wordList.value(p._1._2).toDouble
        val pmi = Math.log10(p._2 * lineNum / (left * right)).toFloat
        (p._1, (pmi, p._2))
      })

    result.saveAsTextFile(args.output())
  }
}
