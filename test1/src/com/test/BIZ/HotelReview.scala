package com.test.BIZ

import java.net.URI
import java.util.Date

import com.test.Comm.FileMethod
import com.test.{CommMethod, DB}
import com.test.Entity._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * Created by Administrator on 2015/10/22.
 */
class HotelReview {

  val regex = """([a-z]+)\((.*)-([0-9]+),(.*)-([0-9]+)\)""".r
  val wordreg = "([A-Z]*) ([\\*+、：’…_\\-a-zA-Z0-9\u4e00-\u9fa5\uFF00-\uFFFF]+)".r // "([A-Z]*) ([\u4e00-\u9fa5]+)".r

  val savedObjectFileName: String = "hdfs://hadoop:8020/spark/hotelReview/SSRDD_obj1.txt"

  val NOList = List("不", "没", "否", "不太", "没有", "除了")


  def InitRDD(sc: SparkContext): RDD[ShortSentence] = {
    val hrRDD: RDD[ShortSentence] = sc.objectFile(savedObjectFileName)
    // hrRDD.persist( StorageLevel(false,true,false,true,1))
    hrRDD.cache()
    hrRDD
  }

  //获取按酒店统计的词出现次数
  def GetHotelWordCountRDD(hrRDD: RDD[ShortSentence], wordsList: List[String]): RDD[(String, Int)] = {
    hrRDD.flatMap(hr => (for (item <- hr.RelList if wordsList.contains(item.Word2)) yield (hr.hotelid + ":" + item.Word2, 1))).reduceByKey(_ + _)
  }

  def GenHotelKeyWordRelWordEntity1(hr: ShortSentence, item: RelItem): HotelKeyWordRelWord = {
    val ADV_NO = GetADVAndNO(hr, item.Word1, item.Word2)
    HotelKeyWordRelWord(hr.hotelid, item.Word1, item.Word2, item.w2POS, ADV_NO._1, ADV_NO._2, ADV_NO._3, ADV_NO._4)
  }

  def GenHotelKeyWordRelWordEntity2(hr: ShortSentence, item: RelItem): HotelKeyWordRelWord = {
    val ADV_NO = GetADVAndNO(hr, item.Word2, item.Word1)
    HotelKeyWordRelWord(hr.hotelid, item.Word2, item.Word1, item.w1POS, ADV_NO._1, ADV_NO._2, ADV_NO._3, ADV_NO._4)
  }

  def GetHotelKeyWordRelWordRDD(hrRDD: RDD[ShortSentence], keyWordList: List[KeyWordRelWordEntity]): RDD[(String, Int)] = {
    val krList = keyWordList.map(o => o.KeyWord + ":" + o.RelWord + ":" + o.RelWordPOS)
    val ssKey1 = hrRDD.flatMap(hr => for (item <- hr.RelList if (krList.contains(item.Word1 + ":" + item.Word2 + ":" + item.w2POS))) yield (hr, item))
    val ssKey2 = hrRDD.flatMap(hr => for (item <- hr.RelList if (krList.contains(item.Word2 + ":" + item.Word1 + ":" + item.w1POS))) yield (hr, item))

    val relWordList1 = ssKey1.map(hi => new HotelReview().GenHotelKeyWordRelWordEntity1(hi._1, hi._2))

    val relWordList2 = ssKey2.map(hi => new HotelReview().GenHotelKeyWordRelWordEntity2(hi._1, hi._2))

    val relWordList = relWordList1.union(relWordList2)

    relWordList.map(s => (s.hotelid + ":" + s.KeyWord + ":" + s.RelWord + ":" + s.RelWordPOS + ":" + s.ADV + ":" + s.ADVPOS + ":" + s.NO + ":" + s.NOPOS, 1)).reduceByKey(_ + _)

  }

  //获取词的出现次数
  def GetWordCountRDD(hrRDD: RDD[ShortSentence], wordsList: List[String]): RDD[(String, Int)] = {
    hrRDD.flatMap(hr => (for (item <- hr.RelList if wordsList.contains(item.Word2)) yield (item.Word2, 1))).reduceByKey(_ + _)
  }

  //获取词的依存关系词
  def GetKeyWordRelWordRDD(hrRDD: RDD[ShortSentence], keyWordList: List[String]): RDD[(String, Int)] = {

    /*   val keyWordRdd = sc.parallelize(list).map(kw=>(kw.ID,kw.KeyWord)).collectAsMap
       var broadCastMap = sc.broadcast(keyWordRdd) //save table1 as map, and broadcast it*/

    val ssKey1: RDD[KeyRelWordItem] = hrRDD.flatMap(hr => for (item <- hr.RelList if (keyWordList.contains(item.Word1))) yield (hr, item)).map(hi => {
      try {
        new KeyRelWordItem(hi._2.Word1, hi._2.Word2, hi._2.w2POS)
      }
      catch {
        case unknown => new KeyRelWordItem("", "", "")
      }
    })


    val ssKey2: RDD[KeyRelWordItem] = hrRDD.flatMap(hr => for (item <- hr.RelList if (keyWordList.contains(item.Word2))) yield (hr, item)).map(hi => {
      try {
        new KeyRelWordItem(hi._2.Word2, hi._2.Word1, hi._2.w1POS)
      }
      catch {
        case unknown => new KeyRelWordItem("", "", "")
      }
    })

    val ssKey = ssKey1.union(ssKey2)

    ssKey.map(s => (s.Word + ":" + s.RelWord + ":" + s.RelWordPOS, 1)).reduceByKey(_ + _)
  }


  def GetADVAndNO(hr: ShortSentence, KeyWord: String, RelWord: String): (String, String, String, String) = {
    var ADV: String = ""
    var NO: String = ""
    var ADVPOS: String = ""
    var NOPOS: String = ""
    for (item <- hr.RelList) {
      if (item.Word1 == RelWord && item.Word2 != KeyWord) {
        if (item.w2POS == "AD") {
          ADV = item.Word2
          ADVPOS = item.w2POS
        }
        if (NOList.contains(item.Word2)) {
          NO = item.Word2
          NOPOS = item.w2POS
        }
      }
      else if (item.Word2 == RelWord && item.Word1 != KeyWord) {
        if (item.w1POS == "AD") {
          ADV = item.Word1
          ADVPOS = item.w1POS
        }
        if (NOList.contains(item.Word1)) {
          NO = item.Word1
          NOPOS = item.w1POS
        }
      }
    }

    (ADV, ADVPOS, NO, NOPOS)
  }


  def parseWordItem(str: String): List[WordItem] = {
    try {
      var list = (for (wordreg(pos, word) <- wordreg.findAllIn(str)) yield (pos, word)).map(item => WordItem(item._2, item._1)).toList
      List(WordItem("ROOT", "")) ++ list
    }
    catch {
      case ex: scala.MatchError => List()
    }
  }

  def parseRelItem(str1: String, strings: Array[String]): List[RelItem] = {
    try {
      val wordList: List[WordItem] = parseWordItem(str1)
      strings.map(str => {
        val regex(rel, w1, w1index, w2, w2index) = str
        if (wordList.length > w1index.toInt && wordList.length > w2index.toInt) {
          RelItem(rel, w1.trim, wordList(w1index.toInt).POS, w1index.toInt, w2.trim, wordList(w2index.toInt).POS, w2index.toInt)
        }
        else {
          RelItem("", "", "", 0, "", "", 0)
        }
      }).toList

    }
    catch {
      case ex: scala.MatchError => List()
    }
  }

  def transSS(str: String): ShortSentence = {
    if (str.length > 10) {
      val item: Array[String] = str.split('\t')
      val idx: Long = item(0).toLong
      val hotelid: Int = item(1).toInt
      val writing: Int = item(2).toInt
      val relList: List[RelItem] = parseRelItem(item(3), item.takeRight(item.length - 4))
      ShortSentence(idx, hotelid, writing, relList)
    }
    else {
      ShortSentence(0, 0, 0, null)
    }
  }

  def GetKeyWordList: List[String] = {
    val list: Seq[KeyWordEntity] = (new DB()).GetKeyWordsList();
    list.map(l => (l.KeyWord)).toList
  }

  def CreateSSObject(soruceFilePath: String, saveFilePath: String, sc: SparkContext): RDD[ShortSentence] = {
    val hrRDD = sc.textFile(soruceFilePath).map(s => new HotelReview().transSS(s)).filter(s => s.idx > 0)
    hrRDD.saveAsObjectFile(saveFilePath)
    hrRDD
  }

  def GenKeyWordRelWordData(hrRDD: RDD[ShortSentence], targetFilePath: String, targetFileName: String) = {

    val keyWordList = GetKeyWordList

    /*   val keyWordRdd = sc.parallelize(list).map(kw=>(kw.ID,kw.KeyWord)).collectAsMap
       var broadCastMap = sc.broadcast(keyWordRdd) //save table1 as map, and broadcast it*/


    val ssKey1: RDD[KeyRelWordItem] = hrRDD.flatMap(hr => for (item <- hr.RelList if (keyWordList.contains(item.Word1))) yield (hr, item)).map(hi => {
      try {
        new KeyRelWordItem(hi._2.Word1, hi._2.Word2, hi._2.w2POS)
      }
      catch {
        case unknown => new KeyRelWordItem("", "", "")
      }
    })


    val ssKey2: RDD[KeyRelWordItem] = hrRDD.flatMap(hr => for (item <- hr.RelList if (keyWordList.contains(item.Word2))) yield (hr, item)).map(hi => {
      try {
        new KeyRelWordItem(hi._2.Word2, hi._2.Word1, hi._2.w1POS)
      }
      catch {
        case unknown => new KeyRelWordItem("", "", "")
      }
    })

    val ssKey = ssKey1.union(ssKey2)

    val ssKeyCount = ssKey.map(s => (s.Word + ":" + s.RelWord + ":" + s.RelWordPOS, 1)).reduceByKey(_ + _)

    new FileMethod().deleteHDFSFold(targetFilePath, targetFileName)

    ssKeyCount.map(i => i._1 + ":" + i._2.toString).saveAsTextFile(targetFilePath + targetFileName)

  }


  // batch insert KeyWord RelWord into DB from File
  def InsertRelWordDataFromFile(dataList: RDD[(String, Int)]) = {
    val valueList = dataList.map(line => {
      val datas = line._1.split(":")
      "('" + datas(0) + "' ,'" + datas(1) + "' ,'" + datas(2) + "',0 ,GetDate() ," + line._2 + " )";
    }).collect()

    new DB().InsertRelWordBatch(valueList.toList)
  }

  def InsertHotelRelWordDataFromFile(dataList: RDD[(String, Int)]) = {
    val valueList = dataList.map(line => {
      val datas = line._1.split(":")
      "(" + datas(0) + " ,'" + datas(1) + "' ,'" + datas(2) + "','" + datas(3) + "','" + datas(4) + "','" + datas(5) + "','" + datas(6) + "','" + line._2 + "' )"
    }).collect()

    new DB().InsertHotelKeyWordRelWordBatch(valueList.toList)
  }

}
