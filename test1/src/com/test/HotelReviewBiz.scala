package com.test

import java.io.{PushbackInputStream, File}
import java.net.URI
import java.util
import java.util.Date
import com.test.BIZ._
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

  def RDDCcount(cmd: String, hrRDD: RDD[ShortSentence]) = {

    val maxHotelID = cmd.toInt
    println(">>>>>>>>>>>>>>>>>>>>>>>> Hotel Count:" + hrRDD.filter(hr => hr.hotelid < maxHotelID).count())

  }

  def autoRun(): Unit =
  {
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "com.test.Comm.HotelReviewKryoRegistrator")

    val sc = new SparkContext()

    val hr = new HotelReview();

  //  hr.CreateTestRDDObjectFile(sc)

   val hrRDD = hr.InitRDD(sc)
  //val hrRDD = hr.InitTestRDD(sc)

    while(true)
    {
      val cmdList: Seq[SparkCmdEntity] =  new DB().GetTaskCmdList()

      if( cmdList.length > 0) {

        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> New Command <<<<<<<<<<<<<<<<<<<<<<<<<<<")
        cmdList.foreach(c=>println(c.IDX + ":" + c.Type + ":" + c.Description + ":" + c.InputData))

        val cmdTypeList = cmdList.map(c => c.Type).distinct

        cmdTypeList.foreach(typeID => {
          val typeCmdList = cmdList.filter(o => o.Type == typeID)
          typeID match {
            case 1 => hr.CalHotelKeyWordCount(hrRDD, typeCmdList)
            case 2 => hr.CalHotelKeyWordRelWord(hrRDD, typeCmdList)
            case 3 => hr.CalKeyWordRelWord(hrRDD, typeCmdList)
            case 4 => hr.CalHotelGroupKeyWord(hrRDD,typeCmdList)
            case 5 => hr.CalHotelGroupKeyWordWithWriting(hrRDD,typeCmdList)
            case 6 => hr.CalHotelKeyWordCountsWithExp(hrRDD,typeCmdList)
            case 7 => hr.CalHotelKeyWordCountsWithWriting(hrRDD,typeCmdList)
            case _ => println("I don't know how to deal the type " + typeID +", can you tell me?")
          }

        })
      }

      print(".")

      Thread.sleep(5000)
    }

  }

  def  gen2013: Unit ={
    val sc = new SparkContext()

    val savedObjectFileName: String = "hdfs://hadoop:8020/spark/hotelReview/SSRDD_obj1.txt"
    val saved2013ObjectFileName: String = "hdfs://hadoop:8020/spark/hotelReview/SSRDD_13_obj.txt"
    val writing2013:String = "hdfs://hadoop:8020/spark/hotelReview/writing2013.txt"
    val hrRDD: RDD[ShortSentence] = sc.objectFile(savedObjectFileName)

    val wRDD = sc.textFile(writing2013).map(w=>(w,0))

     hrRDD.keyBy(_.writing.toString).join(wRDD).map(_._2._1).saveAsObjectFile(saved2013ObjectFileName)
  }

  def testExpress(): Unit ={

    val str ="【good】"
    val exp =  "(\"室内\"->\"泳池\")&&(\"儿童\"->\"泳池\")" //"- \"6\" +(\"干嘛\" + \"1\")**(\"2\" - \"3\") / (\"4\" - \"5\") + (!\"7\") + (\"儿童,室内\"->\"泳池\") "
    val e2 = Arith.parse(exp)


    println(exp)
    println(e2)
   val ss:ShortSentence =  ShortSentence(1,2,3,List[RelItem]( RelItem( "","泳池","",1,"室内","",2),RelItem( "","泳池","",1,"儿童","",2) ) )

    println(NLPArith.evaluate(e2,ss))

    return

    val list:List[String] = List("现在","可以","","")
   // val exp = new LogicalExpression(list).sparse("(\"现在\"->\"可以\")||(\"没有\"->\"就好\")")
 //   print(exp.successful)

    val sc = new SparkContext()

    val hr = new HotelReview();

    //  hr.CreateTestRDDObjectFile(sc)

    //val hrRDD = hr.InitRDD(sc)
    val hrRDD = hr.InitTestRDD(sc)

 //   hrRDD.collect().foreach(f=>println(f))

    val fRDD = hrRDD.filter(h=> NLPArith.evaluate(e2,h ))

    println(fRDD.count())

    fRDD.collect().foreach(f=>println(f))

  }

  def main(args: Array[String]) {
  //  testExpress();
    //return ;

     autoRun()
   // run_GenKeyWordRelWord()
    return

    val urlContent = new HRCommMethod().GetURLContent("http://192.168.1.114:808/API/Parse/test?sentence=%E5%AE%A4%E5%86%85%E6%81%92%E6%B8%A9%E6%B8%B8%E6%B3%B3%E6%B1%A0")
    print(urlContent)
    return ;
    test2();
    return

    val sparkConfig = new SparkConf().set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(sparkConfig)

    val hrRDD: RDD[ShortSentence] = InitRDD(sc)

    var cmd: String = new CommMethod().WaitCommand();
    while (cmd != "Q") {
      RDDCcount(cmd, hrRDD);
      cmd = new CommMethod().WaitCommand();

    }

  }


  def InitRDD(sc: SparkContext): RDD[ShortSentence] = {

    val list: Seq[KeyWordEntity] = (new DB()).GetKeyWordsList();
    val keyWordList = list.map(l => (l.KeyWord))

    /*   val keyWordRdd = sc.parallelize(list).map(kw=>(kw.ID,kw.KeyWord)).collectAsMap
    var broadCastMap = sc.broadcast(keyWordRdd) //save table1 as map, and broadcast it*/

    val filePath = "hdfs://hadoop:8020/spark/testData1"

    val hrRDD: RDD[ShortSentence] = sc.textFile(filePath).map(s => new HotelReview().transSS(s)).filter(s => s.idx > 0)
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

    val hrRDD = sc.textFile(filePath).map(s => new HotelReview().transSS(s)).filter(s => s.idx > 0)
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

    val soruceFilePath: String = "hdfs://hadoop:8020/spark/hotelReview2/stanfordWordTag.txt" // "hdfs://hadoop:8020/spark/hotelReview2/610.txt" // "hdfs://hadoop:8020/spark/hotelReview2/stanfordWordTag.txt"
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

    println(">>>>>>>>>>>>>>>>>>>startTime::" + startTime + "   startInitRDDTime:" + startInitRDDTime + " CurDateTime：" + (new Date))


    var cmd: String = new CommMethod().WaitCommand();
    while (cmd != "Q") {
      val time1 = new Date()
      println(cmd + " Count :" + hr.GetHotelWordCountRDD(hrRDD, cmd.split(",").toList).count())
      println(time1)
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

  def GetRDDList:Array[ShortSentence]={
   // (for (line <- Source.fromFile("D:\\LOG\\hotelReview\\398.txt").getLines() if line.length > 10) yield (line)).map(line => new HotelReview().transSS(line)).toArray
    (for (line <- Source.fromFile("D:\\LOG\\hotelReview\\0\\138.txt").getLines() if line.length > 10) yield (line)).map(line => new HotelReview().transSS(line)).toArray
  }

  def test2() = {

    val strSS: String = "1265963\t55\t241310\t(ROOT  (IP    (NP (NN 但) (NN 价格))    (VP      (ADVP (AD 不))      (VP (VC 是)        (VP          (ADVP (AD 很))          (VP (VV 贵)))))))\tnn(价格-2, 但-1)\tnsubj(贵-6, 价格-2)\tneg(贵-6, 不-3)\tcop(贵-6, 是-4)\tadvmod(贵-6, 很-5)\troot(ROOT-0, 贵-6)"


    val list1 = List("很","贵")
    val hr =  new HotelReview().transSS(strSS)
    val bHas = new  HRCommMethod().ContaintAllWordsWithSeq(hr,list1)

    val ssl: Array[ShortSentence] =GetRDDList
    /*val list: Seq[KeyWordEntity] = new DB().GetKeyWordsList()

    val keyWordList = list.map(l=>(l.KeyWord))*/

    val list: Seq[KeyWordRelWordEntity] = new DB().GetKeyWordsRelWordList()

    val krList = list.map(o => o.KeyWord + ":" + o.RelWord + ":" + o.RelWordPOS)

    val krList1 = list.map(o => o.KeyWord + ":" + o.RelWord)

    val ssKey1 = ssl.flatMap(hr => for (item <- hr.RelList if (krList1.contains(item.Word1 + ":" + item.Word2) || krList1.contains(item.Word2 + ":" + item.Word1))) yield (hr, item))

    val hrc = new HRCommMethod()
    val relWordList = ssKey1.map(hi => {
      val hr = hi._1
      val item = hi._2

      if (krList.contains(item.Word1 + ":" + item.Word2 + ":" + item.w2POS)) {
        val ADV_NO = hrc.GetADVAndNO(hr, item.Word1, item.Word2)
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

    new DB().InsertHotelKeyWordRelWordBatch(dataList)
    println()

  }

}
