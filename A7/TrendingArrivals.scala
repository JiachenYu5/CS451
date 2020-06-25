package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._
import org.apache.spark.streaming._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def stateSpecOutput(batchTime: Time, key: String, value: Option[Tuple3[Int, Long, Int]], state: State[Tuple3[Int, Long, Int]]): Option[(String, Tuple3[Int, Long, Int])] = {
    val previousArrival = state.getOption.getOrElse(0, 0L, 0)._1
    val currentArrival = value.getOrElse(0, 0L, 0)._1
    val time = batchTime.milliseconds
    if (currentArrival >= 10 && currentArrival >= previousArrival * 2) {
      if (key == "goldman")
        println(s"Number of arrivals to Goldman Sachs has doubled from $previousArrival to $currentArrival at $time!")
      else
        println(s"Number of arrivals to Citigroup has doubled from $previousArrival to $currentArrival at $time!")
    }
    state.update((currentArrival, time, previousArrival))
    Some((key, (currentArrival, time, previousArrival)))
  }

  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)
    val stateSpecDetail = StateSpec.function(stateSpecOutput _)

    val wc = stream.map(_.split(","))
      .map(tuple => {
        if (tuple(0) == "green") {
          (List(tuple(8).toDouble, tuple(9).toDouble), 1)
        } else {
          (List(tuple(10).toDouble, tuple(11).toDouble), 1)
        }
      })
      .filter(tuple => ((tuple._1(0).toDouble > -74.0144185 && tuple._1(0).toDouble < -74.013777) && 
        (tuple._1(1).toDouble > 40.7138745 && tuple._1(1).toDouble < 40.7152275)) ||
        ((tuple._1(0).toDouble > -74.012083 && tuple._1(0).toDouble < -74.009867) && 
        (tuple._1(1).toDouble > 40.720053 && tuple._1(1).toDouble < 40.7217236))
      )
      .map(tuple => {
        if ((tuple._1(0).toDouble > -74.0144185 && tuple._1(0).toDouble < -74.013777) && 
        (tuple._1(1).toDouble > 40.7138745 && tuple._1(1).toDouble < 40.7152275)) {
          ("goldman", 1)
        } else {
          ("citigroup", 1)
        }
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .map(tuple => (tuple._1, (tuple._2, 0L, 0)))
      .mapWithState(stateSpecDetail)
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}