package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._
import scala.collection.mutable._

class ConfTrainSpamClassifier(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "trained model", required = true)
  val shuffle = opt[Boolean](descr = "shuffle data", required = false, default = Some(false))
  verify()
}

object TrainSpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfTrainSpamClassifier(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
    val shuffle = args.shuffle()
    var textFile = sc.textFile(args.input())

    // w is the weight vector (make sure the variable is within scope)
    val w = Map[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }
    
    // This is the main learner:
    val delta = 0.002

    // shuffle data
    if (shuffle) {
      val randomNum = scala.util.Random
      textFile = textFile.map(line => (randomNum.nextInt, line)).sortByKey().map(line => line._2)
    }
      
    val trained = textFile
      .map(line => {
        val tokens = line.split(" ")
        // Parse input
        val docid = tokens(0)  // document id
        val isSpam = if (tokens(1) == "spam") 1 else 0 // label
        val featureLen = tokens.length - 2
        val features = new Array[Int](featureLen) // feature vector of the training instance
        for ( i <- 0 until featureLen) {
          features(i) = tokens(i + 2).toInt
        }
        (0, (docid, isSpam, features))
      })
      .groupByKey(1)
      // run the trainer
      .map(p => {
        val iterator = p._2.iterator
        while (iterator.hasNext) {
          val instance = iterator.next()
          val isSpam = instance._2
          val features = instance._3
          // Update the weights as follows:
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        }
      })
      .flatMap(p => {
        w.keys.map(key => (key, w(key)))
      })
        
    trained.saveAsTextFile(args.model())
  }
}
