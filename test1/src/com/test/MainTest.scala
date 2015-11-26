package com.test

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileUtil, FileSystem}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkContext
import udm.apache.spark.nlplib.util.UDFMLUtils
import udm.apache.spark.nlplib.util.NLPUtils
import udm.apache.spark.nlplib.util.CommentData

/**
 * Created by ccmicky on 15-9-22.
 */
object MainTest {

  def main(args: Array[String]) {

    if (args.length < 5) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val flag = args(0)
    val kayFilePath: String = args(0) + args(2)
    val Temp: String = "Temp"

    val keytemplatFilePath: String = args(0) + "/" + Temp + args(2)
    val valuetemplatFilePath: String = args(0) + "/" + Temp + args(3)
    val TagedFilePath: String = args(0) + args(1)
    if (flag.toInt>0)
      {
        val sc = new SparkContext()
        val data = UDFMLUtils.loadLibSVMFile(sc, kayFilePath + "/learningData")

        if (args.length > 4) {
          deleteFold(kayFilePath,args(4))
          LearningMethod(sc, kayFilePath + "/train", kayFilePath + args(4)+ "/learningModel")
          //sameModel = SVMModel.load(sc, kayFilePath+args(4)+"/learningModel")
        }
        val sameModel = SVMModel.load(sc, kayFilePath + args(4) + "/learningModel")

        val scoreAndIdx = data.map { point =>
          val score = sameModel.predict(point.features)
          (point.idx, point.hid, point.rid, score)
        }
        val filtedIdex = scoreAndIdx.filter { case (idx: Int, hid: Int, rid: Int, score: Double) =>
          score > 0
        }.map { case (idx, hid, rid, score) => (idx, hid, rid)
        }
        //filtedIdex.saveAsTextFile("./filterid"+getDate.GetNowDate)

        val tagedCollect = NLPUtils.loadTagedFile(sc, TagedFilePath)

        val f = filtedIdex.keyBy(p => p._1)

        val t = tagedCollect.flatMap(_.toMap)
        val filtedtagedCollect = t.join(f)//.map{case (k,v) => v}

        //filtedtagedCollect.saveAsTextFile("./ajjhjshfh")

        val keys = sc.textFile(keytemplatFilePath).map(p => p.trim)
        val bckeys = sc.broadcast(keys.collect)
        val values = sc.textFile(args(0) + "/" + Temp+"/not").map(p => p.trim)
        val bcvalues = sc.broadcast(values.collect)
        println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
/*
        val FalseHotels = filtedtagedCollect.filter { point =>
          BFSearch.isTagedbyKey(bckeys, bcvalues, point._2._1)
        }

        println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

        val TrueHotels = filtedtagedCollect.filter { point =>
          !BFSearch.isTagedbyKey(bckeys, bcvalues, point._2._1)
        }

        val valuewords = sc.textFile(valuetemplatFilePath).map(p => p.trim)
        val bcvaluewords = sc.broadcast(valuewords.collect)
        val Truefeatureidx = TrueHotels.filter(point => BFSearch.isTagedbyKey(bckeys, bcvaluewords, point._2._1))


        val sentwithids = CommentData.getOriginalLearningDataFromFile(sc, kayFilePath + "/swimpool_data.txt")

        val tfi = Truefeatureidx.keyBy(p => p._1)
        val swi = sentwithids.flatMap(_.toMap)
        val filtedsentwithids = swi.join(tfi).map{case(k,v) =>v}

        filtedsentwithids.saveAsTextFile(kayFilePath+args(3)+getDate.GetNowDate)

        delelefromDatabase(args(2).split('/')(1)+args(3).split('/')(1))
        val conn = new DBConn
        conn.OpenConnection
        //val injectRows:Int = conn.ExecuteUpdate("INSERT INTO CommentDB.dbo.classifyHotelbyComment (idx, hid, rid, comment, tagword) VALUES (1, 2, 3, 'ssss' ,'aaaa' )")
        var iterates: Int = 0
        filtedsentwithids.foreach { points =>
          iterates += inserIntoDatebase(points._2._2._2._1,points._2._2._2._2, points._2._2._2._3, points._1._3,args(2).split('/')(1)+args(3).split('/')(1))
        }
        println("insertToatlitemNums:"+iterates.toString)
*/
      }
    else
      {

      }

  }

  def delelefromDatabase(tagword:String){
    val conn = new DBConn
    conn.OpenConnection
    val injectRows:Int = conn.ExecuteUpdate("delete from CommentDB.dbo.classifyHotelbyComment where tagword = '"+tagword+"'")
    print(injectRows)
  }

  def inserIntoDatebase(idx:Int,hid:Int,rid:Int,sent:String,tagword:String) : Int={
    val conn = new DBConn
    conn.OpenConnection
    val injectRows:Int = conn.ExecuteUpdate("INSERT INTO CommentDB.dbo.classifyHotelbyComment (idx, hid, rid, comment, tagword) VALUES ("+idx+","+hid+","+rid+", '"+sent+"', '"+tagword+"' )")
    injectRows
  }


  def LearningMethod(sc:SparkContext,inputpath:String, outputpath:String){
    // Load training data in LIBSVM format.      //iterates
    val data = MLUtils.loadLibSVMFile(sc, inputpath)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 200
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    model.save(sc, outputpath)
    //val sameModel = SVMModel.load(sc, "./myModelPath")
  }

  def deleteFold(path:String,goalpath:String)=
  {
    println("list path:"+path)
    var HDFS = path
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(HDFS),conf)
    val fs = hdfs.listStatus(new Path(path))
    val listPath = FileUtil.stat2Paths(fs)

    for( p <- listPath)
    {
      //if(p.equals())
      println(p.toString)
      //println(p)
      if ((path+goalpath).equals(p.toString))
        {
          println("aaaa")
          hdfs.delete(p)
        }
    }
    println("----------------------------------------")
  }

}
