package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._


class ConfPairs(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

class MyPartitionerBigramPairs(partNum: Int) extends Partitioner {
  override def numPartitions: Int = partNum
  
  override def getPartition(key: Any): Int = key match {
    case null => 0
    case (left, right) => (left.hashCode & Int.MaxValue) % numPartitions
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairs(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyPairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var marginal = 0.0f
    val textFile = sc.textFile(args.input())
    val result = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val Bigram = tokens.sliding(2).map(p => (p(0), p(1))).toList
          val SingleWord = tokens.dropRight(1).map(p => (p, "*")).toList
          Bigram ++ SingleWord
        } else List()
      })
      .map(freq => (freq, 1))
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new MyPartitionerBigramPairs(args.reducers()))
      .map(p => p._1 match {
        case (_, "*") => { 
          marginal = p._2
          (p._1, p._2) 
        }
        case (_, _) => { (p._1, p._2 / marginal) }
      })

    result.saveAsTextFile(args.output())
  }
}