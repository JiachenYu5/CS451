package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class ConfQ6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipping date", required = true)
  val text = opt[Boolean](descr = "text data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet date", required = false, default = Some(false))
  verify()
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ6(argv)

    log.info("Input: " + args.input())
    log.info("Shipping Date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val lineitemFile = sc.textFile(args.input() + "/lineitem.tbl")

      lineitemFile
        .filter(line => {
          val tokens = line.split('|')
          tokens(10) == date
        })
        .map(line => {
          val tokens = line.split('|')
          val quantity = tokens(4).toDouble
          val extendedprice = tokens(5).toDouble
          val discount = tokens(6).toDouble
          val tax = tokens(7).toDouble
          val returnflag = tokens(8)
          val linestates = tokens(9)
          ((returnflag, linestates), (quantity, extendedprice, extendedprice * (1-discount), extendedprice * (1-discount) * (1+tax), discount, 1))
        })
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
        .sortByKey()
        .collect()
        .foreach(p => println((p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1 / p._2._6, p._2._2 / p._2._6, p._2._5 / p._2._6, p._2._6)))
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      
      lineitemRDD
        .filter(line => line(10).toString == date)
        .map(line => {
          val quantity = line.getDouble(4)
          val extendedprice = line.getDouble(5)
          val discount = line.getDouble(6)
          val tax = line.getDouble(7)
          val returnflag = line.getString(8)
          val linestates = line.getString(9)
          ((returnflag, linestates), (quantity, extendedprice, extendedprice * (1-discount), extendedprice * (1-discount) * (1+tax), discount, 1))
        })
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
        .sortByKey()
        .collect()
        .foreach(p => println((p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1 / p._2._6, p._2._2 / p._2._6, p._2._5 / p._2._6, p._2._6)))
    }
  }
}