package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._


class ConfStripesPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold of co-occurrence", required = false, default = Some(1))
  verify()
}

class MyPartitionerStripesPMI(partNum: Int) extends Partitioner {
  override def numPartitions: Int = partNum
  
  override def getPartition(key: Any): Int = key match {
    case null => 0
    case (left, right) => (left.hashCode & Int.MaxValue) % numPartitions
  }
}

class MyPartitionerStripesPMINew(partNum: Int) extends Partitioner {
  override def numPartitions: Int = partNum
  
  override def getPartition(key: Any): Int = key match {
    case null => 0
    case key => (key.hashCode & Int.MaxValue) % numPartitions
  }
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfStripesPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold of co-occurrence: " + args.threshold());

    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val MAX_WORD = 40
    val lineNum = textFile.count()
    val threshold = args.threshold()
    val wordCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(MAX_WORD).toSet
        tokens.map(p => (p, 1)).toList
      })
      .reduceByKey(_ + _)
      .collectAsMap()
    
    val wordList = sc.broadcast(wordCounts)

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
      .repartitionAndSortWithinPartitions(new MyPartitionerStripesPMI(args.reducers()))
      .map(p => {
        val left = wordList.value(p._1._1).toDouble
        val right = wordList.value(p._1._2).toDouble
        val pmi = Math.log10(p._2 * lineNum / (left * right))
        (p._1, (pmi, p._2))
      })
      .map(p => (p._1._1, Map(p._1._2 -> p._2)))
      .partitionBy(new MyPartitionerStripesPMINew(args.reducers()))
      .reduceByKey((mapA, mapB) => mapA.++(mapB))

    result.saveAsTextFile(args.output())
  }
}