package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class ConfQ2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipping date", required = true)
  val text = opt[Boolean](descr = "text data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet date", required = false, default = Some(false))
  verify()
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ2(argv)

    log.info("Input: " + args.input())
    log.info("Shipping Date: " + args.date())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val lineitemFile = sc.textFile(args.input() + "/lineitem.tbl")
      val orderFile = sc.textFile(args.input() + "/orders.tbl")
      val l_table = lineitemFile
        .map(line => (line.split('|')(0).toInt, line.split('|')(10)))
        .filter(p => p._2.toString == date)
      val o_table = orderFile
        .map(line => (line.split('|')(0).toInt, line.split('|')(6)))
      l_table
        .cogroup(o_table)
        .filter(p => p._2._1.nonEmpty)
        .sortByKey()
        .take(20)
        .map(p => (p._2._2.head, p._1.toLong))
        .foreach(println)
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val orderDF = sparkSession.read.parquet(args.input() + "/orders")
      val orderRDD = orderDF.rdd
        
      val l_table = lineitemRDD
        .filter(line => line(10).toString == date)
        .map(p => (p.getInt(0), 1))
      val o_table = orderRDD
        .map(line => (line.getInt(0), line.getString(6)))
      l_table
        .cogroup(o_table)
        .filter(p => p._2._1.nonEmpty)
        .sortByKey()
        .take(20)
        .map(p => (p._2._2.head, p._1.toLong))
        .foreach(println)
    }
  }
}