package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._

class ConfApplyEnsembleSpamClassifier(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "trained model", required = true)
  val method = opt[String](descr = "ensemble method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfApplyEnsembleSpamClassifier(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
    val textFile = sc.textFile(args.input())
    val model_group_x = sc.textFile(args.model() + "/part-00000")
    val model_group_y = sc.textFile(args.model() + "/part-00001")
    val model_britney = sc.textFile(args.model() + "/part-00002")
    val method = args.method()


    // get the weight vector w for group_x model
    val w_table_group_x = model_group_x
      .map(line => {
        val tokens = line.split(",")
        val feature = tokens(0).substring(1).toInt
        val weight = tokens(1).substring(0, tokens(1).length - 1).toDouble
        (feature, weight)
      }).collectAsMap()
    val w_group_x = sc.broadcast(w_table_group_x)

    // get the weight vector w for group_y model
    val w_table_group_y = model_group_y
      .map(line => {
        val tokens = line.split(",")
        val feature = tokens(0).substring(1).toInt
        val weight = tokens(1).substring(0, tokens(1).length - 1).toDouble
        (feature, weight)
      }).collectAsMap()
    val w_group_y = sc.broadcast(w_table_group_y)

    // get the weight vector w for britney model
    val w_table_britney = model_britney
      .map(line => {
        val tokens = line.split(",")
        val feature = tokens(0).substring(1).toInt
        val weight = tokens(1).substring(0, tokens(1).length - 1).toDouble
        (feature, weight)
      }).collectAsMap()
    val w_britney = sc.broadcast(w_table_britney)

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      // score for group x
      var score_group_x = 0d
      features.foreach(f => if (w_group_x.value.contains(f)) score_group_x += w_group_x.value(f))
      // score for group y
      var score_group_y = 0d
      features.foreach(f => if (w_group_y.value.contains(f)) score_group_y += w_group_y.value(f))
      // score for britney
      var score_britney = 0d
      features.foreach(f => if (w_britney.value.contains(f)) score_britney += w_britney.value(f))

      if (method == "average") {
        score = (score_group_x + score_group_y + score_britney) / 3.0
      } else if (method == "vote") {
        var spamCount = 0
        var hamCount = 0
        spamCount += (if (score_group_x > 0) 1 else 0)
        spamCount += (if (score_group_y > 0) 1 else 0)
        spamCount += (if (score_britney > 0) 1 else 0)
        hamCount = 3 - spamCount
        score = spamCount - hamCount
      }
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