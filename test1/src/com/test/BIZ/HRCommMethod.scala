package com.test.BIZ

import com.test.Entity.ShortSentence


/**
 * Created by Administrator on 2015/10/30.
 */
class HRCommMethod {

  val NOList = List("不", "没", "否", "不太", "没有", "除了")

  //获取否定词
  def GetNO(hr: ShortSentence, KeyWord: String): String = {
    var NO: String = ""
    for (item <- hr.RelList) {
      if (item.Word2 == KeyWord) {
        if (NOList.contains(item.Word1)) {
          NO = item.Word1
        }
      }
      else if (item.Word1 == KeyWord) {
        if (NOList.contains(item.Word2)) {
          NO = item.Word2
        }
      }
    }
    NO
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
      if(ADV == NO) //对于 “不好” 这种词 ADV和NO都会是“不”，需要去除
      {
        ADV = ""
        ADVPOS=""
      }
    }

    (ADV, ADVPOS, NO, NOPOS)
  }

}
