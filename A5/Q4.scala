package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class ConfQ4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipping date", required = true)
  val text = opt[Boolean](descr = "text data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet date", required = false, default = Some(false))
  verify()
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ4(argv)

    log.info("Input: " + args.input())
    log.info("Shipping Date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val lineitemFile = sc.textFile(args.input() + "/lineitem.tbl")
      val orderFile = sc.textFile(args.input() + "/orders.tbl")
      val customerFile = sc.textFile(args.input() + "/customer.tbl")
      val nationFile = sc.textFile(args.input() + "/nation.tbl")

      val o_table = orderFile
        .map(line => (line.split('|')(0).toInt, line.split('|')(1).toInt))

      val c_table = customerFile
        .map(line => (line.split('|')(0).toInt, line.split('|')(3).toInt)).collectAsMap()
      val c_tableMap = sc.broadcast(c_table)

      val n_table = nationFile
        .map(line => (line.split('|')(0).toInt, line.split('|')(1))).collectAsMap()
      val n_tableMap = sc.broadcast(n_table)

      lineitemFile
        .filter(line => {
          val tokens = line.split('|')
          tokens(10) == date
        })
        .map(line => (line.split('|')(0).toInt, 1))
        .reduceByKey(_ + _)
        .cogroup(o_table)
        .filter(p => p._2._1.nonEmpty)
        .map(p => (c_tableMap.value(p._2._2.iterator.next()), p._2._1.iterator.next()))
        .reduceByKey(_ + _)
        .map(p => (p._1, (n_tableMap.value(p._1), p._2)))
        .sortByKey()
        .collect()
        .foreach(p => println((p._1,p._2._1,p._2._2)))
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val orderDF = sparkSession.read.parquet(args.input() + "/orders")
      val orderRDD = orderDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd

      val o_table = orderRDD
        .map(line => (line.getInt(0), line.getInt(1)))

      val c_table = customerRDD
        .map(line => (line.getInt(0), line.getInt(3))).collectAsMap()
      val c_tableMap = sc.broadcast(c_table)

      val n_table = nationRDD
        .map(line => (line.getInt(0), line.getString(1))).collectAsMap()
      val n_tableMap = sc.broadcast(n_table)
      
      lineitemRDD
        .filter(line => line(10).toString == date)
        .map(line => (line.getInt(0), 1))
        .reduceByKey(_ + _)
        .cogroup(o_table)
        .filter(p => p._2._1.nonEmpty)
        .map(p => (c_tableMap.value(p._2._2.iterator.next()), p._2._1.iterator.next()))
        .reduceByKey(_ + _)
        .map(p => (p._1, (n_tableMap.value(p._1), p._2)))
        .sortByKey()
        .collect()
        .foreach(p => println((p._1,p._2._1,p._2._2)))
    }
  }
}