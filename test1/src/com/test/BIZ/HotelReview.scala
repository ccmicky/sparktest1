package com.test.BIZ

import java.net.URI
import java.util
import java.util.Date

import com.test.Comm.FileMethod
import com.test.{BFSearch, LogicalExpression1, CommMethod, DB}
import com.test.Entity._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

/**
 * Created by Administrator on 2015/10/22.
 */
class HotelReview {
  def CalHotelKeyWordCountsWithExp(hrRDD: RDD[ShortSentence], typeCmdList: Seq[SparkCmdEntity]) = {

    typeCmdList.foreach(c=>{

      val StartTime = new Date
      val expTree = Arith.parse(c.InputData)
      val KeyWordList:Array[String] = c.InputData.split("+-*/->&|()\"".toCharArray)

      val hkcList = hrRDD.filter(h=> NLPArith.evaluate(expTree,h )).map(hi => {
       val NO =  new HRCommMethod().GetNOWithKeyWordList(hi,KeyWordList)
        HotelWordCountWithWriting(hi.hotelid, "", NO, hi.writing)
      })

      InsertHotelKeyWordCountDataByRuleWithWriting(hkcList,c)

      UpdateOneSparCmdEntity(c, StartTime)
    })

  }

  def CalHotelKeyWordCountsWithWriting(hrRDD: RDD[ShortSentence], typeCmdList: Seq[SparkCmdEntity]) = {
    val StartTime = new Date
    val list = typeCmdList.flatMap(c=>  c.InputData.split(";")).toList

    val hkcList: RDD[HotelWordCountWithWriting] =GetHotelWordCountWithWriting(hrRDD,list)
    InsertHotelKeyWordCountDataWithWriting(hkcList,typeCmdList)

    UpdateSparCmdEntity(typeCmdList, StartTime)
  }


  val regex = """([a-z]+)\((.*)-([0-9]+),(.*)-([0-9]+)\)""".r
  val wordreg = "([A-Z]*) ([\\*+、：’…_\\-a-zA-Z0-9\u4e00-\u9fa5\uFF00-\uFFFF]+)".r // "([A-Z]*) ([\u4e00-\u9fa5]+)".r

  val savedObjectFileName: String ="hdfs://hadoop:8020/spark/hotelReview/SSRDD_13_obj2.txt"// "hdfs://hadoop:8020/spark/hotelReview/SSRDD_13_obj1.txt" // "hdfs://hadoop:8020/spark/hotelReview/SSRDD_obj1.txt"
  val savedTestObjectFileName: String = "hdfs://hadoop:8020/spark/hotelReview/SSRDD_test_obj.txt"


  def GetHotelGroupKeyWordWithWritingRDD(hrRDD: RDD[ShortSentence], list: List[String]): RDD[(Int, Int)] = {
     hrRDD.filter(hr => new HRCommMethod().ContaintAllWordsWithSeq(hr, list)).map(hr => (hr.hotelid,hr.writing))
  }

   def InsertSparkCmdResultWithWritingData(TaskID:Int, dataList:  RDD[(Int, Int)]) = {
     val valueList = dataList.collect().map(line => {
       "(" + TaskID + "," + line._1 + " ," + line._2 + " )"
     })

     new DB().InsertSparkCmdResultWithWritingBatch(TaskID, valueList.toList)
   }

  def CalHotelGroupKeyWordWithWriting(hrRDD: RDD[ShortSentence], typeCmdList: Seq[SparkCmdEntity]) = {
    typeCmdList.foreach(cmd=>{
      val StartTime = new Date
      val list = cmd.InputData.split(",").toList
      val krList =  GetHotelGroupKeyWordWithWritingRDD(hrRDD,list)
      InsertSparkCmdResultWithWritingData(cmd.IDX, krList)
      UpdateSparCmdEntity(Seq(cmd), StartTime)
    })
  }

  def GetHotelGroupKeyWordRDD(hrRDD: RDD[ShortSentence], list: List[String]): RDD[(Int, Int)] = {
    hrRDD.filter(hr => new HRCommMethod().ContaintAllWordsWithSeq(hr, list)).map(hr => (hr.hotelid, 1)).reduceByKey(_ + _)
  }

  def InsertHotelGroupKeyWordData(TaskID:Int, dataList:  RDD[(Int, Int)]) = {
    val valueList = dataList.collect().map(line => {
      "(" + TaskID + "," + line._1 + " ," + line._2 + " )"
    })

    new DB().InsertHotelGroupKeyWordBatch(TaskID, valueList.toList)
  }

  def CalHotelGroupKeyWord(hrRDD: RDD[ShortSentence], typeCmdList: Seq[SparkCmdEntity]) = {
    typeCmdList.foreach(cmd=>{
      val StartTime = new Date
      val list = cmd.InputData.split(",").toList
      val krList =  GetHotelGroupKeyWordRDD(hrRDD,list)
      InsertHotelGroupKeyWordData(cmd.IDX, krList)
      UpdateSparCmdEntity(Seq(cmd), StartTime)
    })
  }


  def CalKeyWordRelWord(hrRDD: RDD[ShortSentence], typeCmdList: Seq[SparkCmdEntity])=
  {
    val StartTime = new Date
    val list = typeCmdList.flatMap(c=>  c.InputData.split(";")).toList
    val krList =  GetKeyWordRelWordRDD(hrRDD,list)
    InsertRelWordData(krList)

    UpdateSparCmdEntity(typeCmdList, StartTime)
  }

  def UpdateSparCmdEntity(typeCmdList: Seq[SparkCmdEntity], StartTime: Date) = {
    typeCmdList.foreach(c=> {
      c.StartTime =  CommMethod.DateToStr(StartTime)
      c.EndTime =  CommMethod.DateToStr(new Date())
      c.State = 1
    }
    )
    new DB().UpdateTaskCmd(typeCmdList)

    typeCmdList.foreach(c=> new HRCommMethod().NotifySparkCmdTaskFinished(c.IDX))

  }

  def UpdateOneSparCmdEntity(typeCmd: SparkCmdEntity, StartTime: Date) = {
    typeCmd.StartTime =  CommMethod.DateToStr(StartTime)
    typeCmd.EndTime =  CommMethod.DateToStr(new Date())
    typeCmd.State = 1

    new DB().UpdateOneTaskCmd(typeCmd)
  }

  def CalHotelKeyWordRelWord(hrRDD: RDD[ShortSentence], typeCmdList: Seq[SparkCmdEntity]) = {

    val StartTime = new Date
    val list: List[KeyWordRelWordEntity] = typeCmdList.flatMap(cmd => cmd.InputData.split(";").map(line => {
      val data = line.split(";")
      val e = new KeyWordRelWordEntity()
      e.KeyWord = data(0)
      e.RelWord = data(1)
      e.RelWordPOS = data(2)
      e
    })).toList

    val hkrList: RDD[(String, Int)] = GetHotelKeyWordRelWordRDD(hrRDD, list.toList)
    InsertHotelRelWordData(hkrList)

    UpdateSparCmdEntity(typeCmdList, StartTime)
  }

  def CalHotelKeyWordCount(hrRDD: RDD[ShortSentence], typeCmdList: Seq[SparkCmdEntity]) = {
    val StartTime = new Date
    val list = typeCmdList.flatMap(c=>  c.InputData.split(";")).toList

    val hkcList: RDD[(String, Int)] = GetHotelWordCountRDD(hrRDD,list)
    InsertHotelKeyWordCountData(hkcList)

    UpdateSparCmdEntity(typeCmdList, StartTime)
  }

  def InitTestRDD(sc: SparkContext): RDD[ShortSentence] = {
    val hrRDD: RDD[ShortSentence] = sc.objectFile(savedTestObjectFileName)
    //hrRDD.persist( StorageLevel(false,true,false,true,1))
    hrRDD.persist(StorageLevel.MEMORY_ONLY)
    hrRDD
  }

  def InitRDD(sc: SparkContext): RDD[ShortSentence] = {
    val hrRDD: RDD[ShortSentence] = sc.objectFile(savedObjectFileName)
    //hrRDD.persist( StorageLevel(false,true,false,true,1))
    hrRDD.persist(StorageLevel.MEMORY_ONLY)
    hrRDD
  }

  //获取按酒店统计的词出现次数
  def GetHotelWordCountRDD(hrRDD: RDD[ShortSentence], wordsList: List[String]): RDD[(String, Int)] = {
    hrRDD.flatMap(hr => (for (item <- hr.RelList if wordsList.contains(item.Word2)) yield (hr.hotelid + ":" + item.Word2, 1))).reduceByKey(_ + _)
  }

  def GenHotelKeyWordRelWordEntity1(hr: ShortSentence, item: RelItem): HotelKeyWordRelWord = {
    val ADV_NO = new HRCommMethod().GetADVAndNO(hr, item.Word1, item.Word2)
    HotelKeyWordRelWord(hr.hotelid, item.Word1, item.Word2, item.w2POS, ADV_NO._1, ADV_NO._2, ADV_NO._3, ADV_NO._4)
  }

  def GenHotelKeyWordRelWordEntity2(hr: ShortSentence, item: RelItem): HotelKeyWordRelWord = {
    val ADV_NO = new HRCommMethod().GetADVAndNO(hr, item.Word2, item.Word1)
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


  //计算某些词在酒店中的命中数量
  def GetHotelWordCountWithWriting(hrRDD: RDD[ShortSentence], keyWordList: List[String]): RDD[HotelWordCountWithWriting] = {

    val ssKey1 = hrRDD.flatMap(hr => for (item <- hr.RelList if (keyWordList.contains(item.Word2))) yield (hr, item))

    ssKey1.map(hi => {
      val hr = hi._1
      val item = hi._2
      val NO = new HRCommMethod().GetNO(hr, item.Word1)
      HotelWordCountWithWriting(hr.hotelid, item.Word2, NO, hr.writing)
    })
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
  def InsertRelWordData(dataList: RDD[(String, Int)]) = {
    val valueList = dataList.map(line => {
      val datas = line._1.split(":",-1)
      "('" + datas(0) + "' ,'" + datas(1) + "' ,'" + datas(2) + "',0 ,GetDate() ," + line._2 + " )";
    }).collect()

    new DB().InsertRelWordBatch(valueList.toList)
  }

  def InsertHotelRelWordData(dataList: RDD[(String, Int)]) = {
    val valueList = dataList.collect().map(line => {
       val datas = line._1.split(":",-1)
      "(" + datas(0) + " ,'" + datas(1) + "' ,'" + datas(2) + "','" + datas(3) + "','" + datas(4) + "','" + datas(5) + "','" + datas(6) + "','" + line._2 + "' )"
    })

    new DB().InsertHotelKeyWordRelWordBatch(valueList.toList)
  }

  def InsertHotelKeyWordCountData(dataList: RDD[(String, Int)]) = {
    val valueList = dataList.collect().map(line => {
      val datas = line._1.split(":",-1)
      "(" + datas(0) + " ,'" + datas(1) + "' ,'" + datas(2) + "','" +  line._2 + "' )"
    })

    new DB().InsertHotelKeyWordCountBatch(valueList.toList)
  }

  def CheckTaskIDX(keyWord: String, typeCmdList: Seq[SparkCmdEntity]):Int = {
    typeCmdList.foreach(c => {
      if (c.InputData.split(";").contains(keyWord))
        return c.IDX
    })
    0
  }

  def InsertHotelKeyWordCountDataByRuleWithWriting(dataList: RDD[HotelWordCountWithWriting], typeCmd: SparkCmdEntity) = {
    val valueList = dataList.collect().map(line => {
      "(" + line.hotelid + " ,'" + line.KeyWord + "' ,'" + line.NO + "','" +  line.Writing + "','" +  typeCmd.IDX + "' )"
    })
    new DB().DelHotelKeyWordCountByTaskID(typeCmd.IDX)
    new DB().InsertHotelKeyWordCountBatchWithWriting(valueList.toList)
  }

  ///////////////////////////////////////////////////////////
  //author: ccmicky
  //parameter: @dataList the result after filtting
  //          @CmdIDX the number of command Type
  //usage: 将结果批量插入数据库
  ///////////////////////////////////////////////////////////
  def InsertHotelKeyWordCountDataByRuleWithWriting(dataList: RDD[HotelWordCountWithWriting], CmdIDX: Int) = {
    val valueList = dataList.collect().map(line => {
      "(" + line.hotelid + " ,'" + line.KeyWord + "' ,'" + line.NO + "','" +  line.Writing + "','" +  CmdIDX + "' )"
    })

    new DB().InsertHotelKeyWordCountBatchWithWriting(valueList.toList)
  }

  def InsertHotelKeyWordCountDataWithWriting(dataList: RDD[HotelWordCountWithWriting], typeCmdList: Seq[SparkCmdEntity]) = {
    val valueList = dataList.collect().map(line => {
      val TaskID = CheckTaskIDX(line.KeyWord,typeCmdList)
      "(" + line.hotelid + " ,'" + line.KeyWord + "' ,'" + line.NO + "','" +  line.Writing + "','" +  TaskID + "' )"
    })
    typeCmdList.foreach(c=> new DB().DelHotelKeyWordCountByTaskID(c.IDX))

    new DB().InsertHotelKeyWordCountBatchWithWriting(valueList.toList)
  }

  def CreateTestRDDObjectFile(sc:SparkContext) {
    val filePath ="hdfs://hadoop:8020/spark/hotelReview/138.txt"

    val hrRDD = sc.textFile(filePath).map(s => new HotelReview().transSS(s)).filter(s => s.idx > 0)

    hrRDD.saveAsObjectFile(savedTestObjectFileName)
  }
  ///////////////////////////////////////////////////////////
  //author: ccmicky
  //parameter: @hrRDD the initial data of stanfordparser result
  //           @tempRDD the initial of the template of the keywords
  //           @typeCmdList the queue of the input cmds
  //usage: 根据输入的命令类型解析表达式对酒店分类
  ///////////////////////////////////////////////////////////
  def ClassifyHotelTypeWithWriting(hrRDD: RDD[ShortSentence], tempRDD: List[Map[String,String]],typeCmdList: Seq[SparkCmdEntity]) = {
    val StartTime = new Date
    val cmdlist = typeCmdList.map(c=>  (c.InputData,c.IDX)).toList
    for (cmd <- cmdlist)
    {
      inputFormatCmd(cmd._1,cmd._2,hrRDD,tempRDD)
    }
    UpdateSparCmdEntity(typeCmdList, StartTime)

  }

  ///////////////////////////////////////////////////////////
  //author: ccmicky
  //parameter: @hrRDD the initial data of stanfordparser result
  //           @tempRDD the initial of the template of the keywords
  //           @cmd the string of the cmd
  //usage:根据有格式输入命令进行表达式解析，对酒店点评进行过滤
  ///////////////////////////////////////////////////////////
  def inputFormatCmd(cmd:String,cmdidx:Int, hrRDD: RDD[ShortSentence], tempRDD: List[Map[String,String]])= {
    println("testing parser")

    //val variables = Map("a" -> true, "b" -> false, "c" -> true)
    val variableParser = LogicalExpression1.sparse(hrRDD,tempRDD) _
    val patten = "[\\u4e00-\\u9fa5_A-Za-z0-9]+".r
    var str = cmd
    var goal = ""
    str = str.replaceAll(" ","")
    for (matchString <- patten.findAllIn(str))
    {
      str = str.replaceAll(matchString, "\""+matchString+"\"")
      goal = matchString
    }
    //println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"+goal)

    while(str.indexOf("\"\"")>=0)
    {
      str = str.replace("\"\"","\"")
    }

    print(str)
    //variableParser("!(\"aaa\"->\"再见\") && !(\"aaa\"->\"ccc\")")
    val filterhrRDD = variableParser(str)

    //val nottemplatFilePath: String ="hdfs://hadoop:8020/spark/ccmicky/Temp/not"
    val not =  tempRDD.filter(p=>p.keys.head=="not")
    var bcnot = List("没有","没","无").toArray
    if(not.length>0)
    {
      bcnot= not.map(p=>p.values.head).toArray
      bcnot.foreach(p=>println("not"+p))
    }

    val goaltemp =  tempRDD.filter(p=> p.keys.head==goal)
    var bcgoal = List(goal).toArray
    if(goaltemp.length>0)
    {
      bcgoal= goaltemp.map(p=>p.values.head).toArray
      bcgoal.foreach(p=>println(goal+p))

    }

    //val bcnot = List("没有").toArray

    val formatedRDD = filterhrRDD.map{p=>
      var flag = ""
      if(BFSearch.isTagedbyKey(bcnot,bcgoal,p.RelList) || BFSearch.isTagedbyKey(bcgoal,bcnot,p.RelList))
      {
        flag = "True"
      }
      HotelWordCountWithWriting(p.hotelid,"",flag,p.writing)
    }
    InsertHotelKeyWordCountDataByRuleWithWriting(formatedRDD,cmdidx)
  }

}
