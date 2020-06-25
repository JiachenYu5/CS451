package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class ConfQ1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipping date", required = true)
  val text = opt[Boolean](descr = "text data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet date", required = false, default = Some(false))
  verify()
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ1(argv)

    log.info("Input: " + args.input())
    log.info("Shipping Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val textFile = sc.textFile(args.input() + "/lineitem.tbl")
      textFile
        .filter(line => {
          val tokens = line.split('|')
          tokens(10) == date
        })
        .map(d => ("key", 1))
        .reduceByKey(_ + _)
        .take(1)
        .foreach(p => println("ANSWER=" + p._2))
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
        
      lineitemRDD
        .filter(line => line(10).toString == date)
        .map(d => ("key", 1))
        .reduceByKey(_ + _)
        .take(1)
        .foreach(p => println("ANSWER=" + p._2))
    }
  }
}
