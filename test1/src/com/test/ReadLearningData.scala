package com.test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import udm.apache.spark.nlplib.regression.PredictPoint

/**
 * Created by ccmicky on 15-9-22.
 */
object ReadLearningData {

  def getOriginalLearningDataFromFile(sc: SparkContext,
                              path: String,
                              numFeatures: Int,
                              minPartitions: Int): RDD[Map[Int,(Int,Int,String)]] = {
    val textFile = sc.textFile(path)
    textFile.map { s =>
      val items = s.split('\t')
      val rid = items(0).toInt
      val hid = items(1).toInt
      val sent = items(2).toString.replace(" ", "")
      val idx = items(3).toInt
      Map(idx->( hid, rid, sent))
    }
  }
  def getOriginalLearningDataFromFile(
                      sc: SparkContext,
                      path: String,
                      numFeatures: Int): RDD[Map[Int,(Int,Int,String)]] =
    getOriginalLearningDataFromFile(sc, path, numFeatures, sc.defaultMinPartitions)

  @deprecated("use method without multiclass argument, which no longer has effect", "1.1.0")
  def getOriginalLearningDataFromFile(
                      sc: SparkContext,
                      path: String,
                      multiclass: Boolean,
                      numFeatures: Int): RDD[Map[Int,(Int,Int,String)]] =
    getOriginalLearningDataFromFile(sc, path, numFeatures)

  @deprecated("use method without multiclass argument, which no longer has effect", "1.1.0")
  def getOriginalLearningDataFromFile(
                      sc: SparkContext,
                      path: String,
                      multiclass: Boolean): RDD[Map[Int,(Int,Int,String)]] =
    getOriginalLearningDataFromFile(sc, path)

  /**
   * Loads binary labeled data in the LIBSVM format into an RDD[PredictPoint], with number of
   * features determined automatically and the default number of partitions.
   */
  def getOriginalLearningDataFromFile(sc: SparkContext, path: String): RDD[Map[Int,(Int,Int,String)]] =
    getOriginalLearningDataFromFile(sc, path, -1)
}
