package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class ConfQ3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipping date", required = true)
  val text = opt[Boolean](descr = "text data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet date", required = false, default = Some(false))
  verify()
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ3(argv)

    log.info("Input: " + args.input())
    log.info("Shipping Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val lineitemFile = sc.textFile(args.input() + "/lineitem.tbl")
      val partFile = sc.textFile(args.input() + "/part.tbl")
      val supplierFile = sc.textFile(args.input() + "/supplier.tbl")

      val p_table = partFile
        .map(line => (line.split('|')(0).toInt, line.split('|')(1))).collectAsMap()
      val p_tableMap = sc.broadcast(p_table)

      val s_table = supplierFile
        .map(line => (line.split('|')(0).toInt, line.split('|')(1))).collectAsMap()
      val s_tableMap = sc.broadcast(s_table)

      lineitemFile
        .filter(line => {
          val tokens = line.split('|')
          tokens(10) == date
        })
        .map(line => (line.split('|')(0).toInt, (line.split('|')(1).toInt, line.split('|')(2).toInt)))
        .sortByKey()
        .take(20)
        .map(p => (p._1, p_tableMap.value(p._2._1), s_tableMap.value(p._2._2)))
        .foreach(println)
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd

      val p_table = partRDD
        .map(line => (line.getInt(0), line.getString(1))).collectAsMap()
      val p_tableMap = sc.broadcast(p_table)

      val s_table = supplierRDD
        .map(line => (line.getInt(0), line.getString(1))).collectAsMap()
      val s_tableMap = sc.broadcast(s_table)
      
      lineitemRDD
        .filter(line => line(10).toString == date)
        .map(line => (line.getInt(0), (line.getInt(1), line.getInt(2))))
        .sortByKey()
        .take(20)
        .map(p => (p._1, p_tableMap.value(p._2._1), s_tableMap.value(p._2._2)))
        .foreach(println)
    }
  }
}