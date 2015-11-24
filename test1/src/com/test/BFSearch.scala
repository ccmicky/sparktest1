package com.test

import com.test.Entity.RelItem
import org.apache.spark.broadcast.Broadcast

/**
 * Created by ccmicky on 15-9-21.
 */

object BFSearch {
  def isTagedbyKey(
                    keys:Array[String],
                    values:Array[String],
                    RelList:List[RelItem]) :Boolean = {
    var mapitem : scala.collection.mutable.Map[String,String] = scala.collection.mutable.Map()
    for (rel <- RelList)
    {
      if(mapitem.contains(rel.Word1))
      {
        mapitem(rel.Word1) += " "+rel.Word2
      }
      else
      {
        mapitem += (rel.Word1 -> rel.Word2)
      }
    }
    //val keys = sc.textFile(keywordfilePath)
    //val values = sc.textFile(valuewordfilePath)
    keys.foreach{ key =>
      values.foreach{value =>
        var isvisitedlist: List[String] = List()
        var klist: List[String] = List(key.trim)
        while (!klist.isEmpty) {
          val keyword = klist.head
          klist = klist.drop(1)
          if (!isvisitedlist.contains(keyword))
          {
            isvisitedlist = isvisitedlist.::(keyword)
            if (mapitem.contains(keyword)) {
              val taglist = mapitem(keyword).split(" ")
              if (taglist.contains(value.trim)){
                return true
              }
              taglist.foreach(s => klist=klist.::(s))
            }
          }
        }
      }
    }
    return false
  }

  def ifcontainsKeyword(
                         keys:Array[String],
                         tagWord:List[RelItem]) :Boolean = {
    //val keys = sc.textFile(keywordfilePath)
    //val values = sc.textFile(valuewordfilePath)
    val result = tagWord.filter(p => keys.contains(p.Word1) ||keys.contains(p.Word2))
    if (result.length >0 )
    {
      return true
    }
    return false
  }

}
