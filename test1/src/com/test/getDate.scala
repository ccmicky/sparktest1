package com.test

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by ccmicky on 15-8-31.
 */
object getDate {
  def GetNowDate: String = {
    var temp_str: String = ""
    val dt: Date = new Date
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    temp_str = sdf.format(dt)
    return temp_str
  }
}