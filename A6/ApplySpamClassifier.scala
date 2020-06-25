package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._

class ConfApplySpamClassifier(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "trained model", required = true)
  verify()
}

object ApplySpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfApplySpamClassifier(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
    val textFile = sc.textFile(args.input())
    val model = sc.textFile(args.model())

    // get the weight vector w
    val w_table = model
      .map(line => {
        val tokens = line.split(",")
        val feature = tokens(0).substring(1).toInt
        val weight = tokens(1).substring(0, tokens(1).length - 1).toDouble
        (feature, weight)
      }).collectAsMap()
    val w = sc.broadcast(w_table)

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.value.contains(f)) score += w.value(f))
      score
    }

    val result = textFile
      .map(line => {
        val tokens = line.split(" ")
        // Parse input
        val docid = tokens(0)  // document id
        val isSpam = tokens(1) // label
        val featureLen = tokens.length - 2
        val features = new Array[Int](featureLen) // feature vector of the training instance
        for ( i <- 0 until featureLen) {
          features(i) = tokens(i + 2).toInt
        }
        val score = spamminess(features)
        var resultIsSpam = "spam"
        if (score <= 0) resultIsSpam = "ham"
        (docid, isSpam, score, resultIsSpam)
      })
      
    result.saveAsTextFile(args.output())
  }
}