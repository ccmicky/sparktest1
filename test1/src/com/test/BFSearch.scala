package com.test

import org.apache.spark.broadcast.Broadcast

/**
 * Created by ccmicky on 15-9-21.
 */

object BFSearch {
  def isTagedbyKey(
                    keys:Broadcast[Array[String]],
                    values:Broadcast[Array[String]],
                    tagWord:Map[String,String]) :Boolean = {
    //val keys = sc.textFile(keywordfilePath)
    //val values = sc.textFile(valuewordfilePath)
    keys.value.foreach{ key =>
      values.value.foreach{value =>
        val isvisitedlist: List[String] = List()
        var klist: List[String] = List(key.trim)
        while (!klist.isEmpty) {
          val keyword = klist.head
          klist = klist.drop(1)
          if (!isvisitedlist.contains(keyword))
          {
            isvisitedlist.::(keyword)
            if (tagWord.contains(keyword)) {

              if (tagWord(keyword).contains(value.trim)) {
                return true
              }
              else {
                value.split(" ").foreach(s => klist.::(s))
              }
            }
          }
        }
      }
    }
    return false
  }

}
