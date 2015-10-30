package com.test

import java.io.{PushbackInputStream, File}
import java.net.URI
import java.util
import java.util.Date
import com.test.BIZ.HotelReview
import com.test.Comm.FileMethod
import com.test.Entity._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileUtil, Path, FileSystem}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.Predef
import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by Administrator on 2015/10/19.
 */
object HotelReviewBiz {

  def RDDCcount(cmd: String , hrRDD: RDD[ShortSentence] )=  {

    val  maxHotelID = cmd.toInt
    println(">>>>>>>>>>>>>>>>>>>>>>>> Hotel Count:" +  hrRDD.filter(hr=> hr.hotelid  < maxHotelID).count() )

  }

  def main(args: Array[String]) {
      run_GenKeyWordRelWord()


 return

    test2();
    return

    val sparkConfig = new SparkConf().set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(sparkConfig)

    val hrRDD :RDD[ShortSentence]  = InitRDD(sc)

     var cmd:String  = new CommMethod().WaitCommand();
    while(cmd !="Q")
    {
       RDDCcount(cmd,hrRDD);
      cmd = new CommMethod().WaitCommand();

    }

  }



  def InitRDD( sc: SparkContext):RDD[ShortSentence] = {

    val list: Seq[KeyWordEntity] = (new DB()).GetKeyWordsList();
    val keyWordList = list.map(l => (l.KeyWord))

    /*   val keyWordRdd = sc.parallelize(list).map(kw=>(kw.ID,kw.KeyWord)).collectAsMap
    var broadCastMap = sc.broadcast(keyWordRdd) //save table1 as map, and broadcast it*/

    val filePath = "hdfs://hadoop:8020/spark/testData1"

    val hrRDD:RDD[ShortSentence] = sc.textFile(filePath).map(s => new HotelReview().transSS(s)).filter(s => s.idx > 0 )
    hrRDD.cache()

    System.out.print(">>>>>>>>>>>>>>>>>hrRDD：" + hrRDD.count().toString)

    hrRDD
  }


  def test1(index: Int, sc: SparkContext) = {


    val list: Seq[KeyWordEntity] = (new DB()).GetKeyWordsList();
    val keyWordList = list.map(l => (l.KeyWord))

    /*   val keyWordRdd = sc.parallelize(list).map(kw=>(kw.ID,kw.KeyWord)).collectAsMap
    var broadCastMap = sc.broadcast(keyWordRdd) //save table1 as map, and broadcast it*/

    val filePath = "hdfs://hadoop:8020/spark/hotelReview/*/" //  List("hdfs://hadoop:8020/spark/hotelReview/0/138.txt","hdfs://hadoop:8020/spark/hotelReview/0/137.txt")

    val hrRDD = sc.textFile(filePath).map(s => new HotelReview().transSS(s)).filter(s => s.idx > 0 )
    System.out.print(">>>>>>>>>>>>>>>>>hrRDD：" + hrRDD.count().toString)


    val ssKey2: RDD[KeyRelWordItem] = hrRDD.flatMap(hr => for (item <- hr.RelList if (keyWordList.contains(item.Word2))) yield (hr, item)).map(hi => {
      try {
        new KeyRelWordItem(hi._2.Word1, hi._2.Word2, hi._2.w2POS)
      }
      catch {
        case unknown => new KeyRelWordItem("", "", "")
      }
    })
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>ssKey2：" + ssKey2.count().toString)


    val ssKey1: RDD[KeyRelWordItem] = hrRDD.flatMap(hr => for (item <- hr.RelList if (keyWordList.contains(item.Word1))) yield (hr, item)).map(hi => {
      try {
        new KeyRelWordItem(hi._2.Word1, hi._2.Word2, hi._2.w2POS)
      }
      catch {
        case unknown => new KeyRelWordItem("", "", "")
      }
    })

    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>ssKey11：" + ssKey1.count().toString)

    val ssKey = ssKey1.union(ssKey2)

    println()
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>ssKey：" + ssKey.count().toString)


    val ssKeyCount = ssKey.map(s => (s.Word + ":" + s.RelWord + ":" + s.RelWordPOS, 1)).reduceByKey(_ + _)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>Begin to InsertRelWordData：" + ssKeyCount.getClass)


    val path: String = "hdfs://hadoop:8020/spark/test/"
    val goalpath = "3.txt"

    new FileMethod().deleteHDFSFold(path, goalpath)

    ssKeyCount.map(i => i._1 + ":" + i._2.toString).saveAsTextFile(path + goalpath)
    val data = sc.textFile(path + goalpath)
    data.foreach(p => println(p))

    //  InsertData(path,goalpath);

    /*var iterates: Int = 0
    ssKeyCount.foreach(s=> {
      val ps = s._1.split(":")
     // iterates += inserIntoDatebase("Good","Good","Good",s._2)
      iterates += inserIntoDatebase(-1,-1,-1,"T","t")
      //  new DB().InsertRelWordData(s._1, s._2)
    }
    )
    println(">>>>>>>>>>>>>>>>>insertToatlitemNums:"+iterates.toString)*/
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>Finished InsertRelWordData：")
    //  ssKeyCount.saveAsTextFile()

    // ssKey.take(100).foreach(s=> println(">>>>>>>>-->>>>>>>>>>" + s.Word +":" + s.RelWord + ":" + s.RelWordPOS ))


  }


  def run_GenKeyWordRelWord(): Unit = {

    val hr = new HotelReview();
    // Make sure to set these properties *before* creating a SparkContext!
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "com.test.Comm.HotelReviewKryoRegistrator")

    val sc = new SparkContext()

    val soruceFilePath: String = "hdfs://hadoop:8020/spark/hotelReview2/stanfordWordTag.txt"// "hdfs://hadoop:8020/spark/hotelReview2/610.txt" // "hdfs://hadoop:8020/spark/hotelReview2/stanfordWordTag.txt"
    /*    val targetFilePath: String = "hdfs://hadoop:8020/spark/test/"
        val targetFileName: String = "6.txt"*/


    val startTime = new Date()
    println(">>>>>>>>>>>>>StartTime:" + startTime)

    //  hr.CreateSSObject(soruceFilePath, savedObjectFileName, sc)


    //hrRDD.persist( StorageLevel(false,true,false,true,1))

    //println(">>>>>>>>>>>>>>> hrRDD Count：" + hrRDD.count())

    val startInitRDDTime = new Date()
    println(">>>>>>>>>>>>>StartInitRDDTime:" + startInitRDDTime)
    val hrRDD = hr.InitRDD(sc)



    //hrRDD.saveAsObjectFile("hdfs://hadoop:8020/spark/hotelReview/SSRDD_obj.txt")

    println(">>>>>>>>>>>>>>>>>>>startTime::" + startTime + "   startInitRDDTime:" +  startInitRDDTime + " CurDateTime：" +(new Date) )


    var cmd:String  = new CommMethod().WaitCommand();
    while(cmd !="Q")
    {
      val time1 = new Date()
      println(cmd + " Count :" + hr.GetHotelWordCountRDD(hrRDD, cmd.split(",").toList).count())
      println(time1  )
      println(new Date())
      cmd = new CommMethod().WaitCommand();
    }

    // hr.GenKeyWordRelWordData(hrRDD, targetFilePath, targetFileName)


    //  hr.InsertRelWordDataFromFile(targetFilePath + targetFileName)
  }


  def checkListIndex(ListLength: Int, Index: Int, hotelID: Int, idx: Long): Boolean = {
    if (Index > ListLength) {
      println(hotelID + ":" + idx)
      false
    }
    else {
      true
    }
  }

  def test2() = {

    val strSS: String = "1265963\t55\t241310\t(ROOT  (IP    (NP (NN 但) (NN 价格))    (VP      (ADVP (AD 不))      (VP (VC 是)        (VP          (ADVP (AD 很))          (VP (VV 贵)))))))\tnn(价格-2, 但-1)\tnsubj(贵-6, 价格-2)\tneg(贵-6, 不-3)\tcop(贵-6, 是-4)\tadvmod(贵-6, 很-5)\troot(ROOT-0, 贵-6)"

    val ssl: Array[ShortSentence] = (for (line <- Source.fromFile("D:\\LOG\\hotelReview\\398.txt").getLines() if line.length > 10) yield (line)).map(line => new HotelReview().transSS(line)).toArray


    /*val list: Seq[KeyWordEntity] = new DB().GetKeyWordsList()

    val keyWordList = list.map(l=>(l.KeyWord))*/

    val list: Seq[KeyWordRelWordEntity] = new DB().GetKeyWordsRelWordList()

    val krList = list.map(o => o.KeyWord + ":" + o.RelWord + ":" + o.RelWordPOS)

    val krList1 = list.map(o => o.KeyWord + ":" + o.RelWord)

    val ssKey1 = ssl.flatMap(hr => for (item <- hr.RelList if (krList1.contains(item.Word1 + ":" + item.Word2) || krList1.contains(item.Word2 + ":" + item.Word1))) yield (hr, item))

    val hrc = new HotelReview()
    val relWordList = ssKey1.map(hi => {
      val hr = hi._1
      val item = hi._2

        if (krList.contains(item.Word1 + ":" + item.Word2 + ":" + item.w2POS)) {
          val ADV_NO =  hrc.GetADVAndNO(hr, item.Word1, item.Word2)
          HotelKeyWordRelWord(hr.hotelid, item.Word1, item.Word2, item.w2POS, ADV_NO._1, ADV_NO._2, ADV_NO._3, ADV_NO._4)

        }
        else {
          val ADV_NO = hrc.GetADVAndNO(hr, item.Word2, item.Word1)
            HotelKeyWordRelWord(hr.hotelid, item.Word2, item.Word1, item.w1POS, ADV_NO._1, ADV_NO._2, ADV_NO._3, ADV_NO._4)
        
        }
    })


    relWordList.foreach(println)


    val dataList = (for (hr <- relWordList) yield hr).map(hr => {
      "(" + hr.hotelid + " ,'" + hr.KeyWord + "' ,'" + hr.RelWord + "','" + hr.RelWordPOS + "','" + hr.ADV + "','" + hr.ADVPOS + "','" + hr.NO + "','" + hr.NOPOS + "' )";
    }).toList

    var index = 0
    var values = ""
    val batchLenght = 500
    dataList.foreach(line => {
      values += line + ","
      index += 1
      if (index > batchLenght) {
        new DB().InsertHotelKeyWordRelWordBatch(values)
        index = 0
        values = ""
      }
    })

    if (values.length > 0) {
      new DB().InsertHotelKeyWordRelWordBatch(values)
    }



    println()

  }

}
