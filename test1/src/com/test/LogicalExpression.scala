package com.test

import com.test.Entity.{RelItem, ShortSentence}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


import scala.util.parsing.combinator.JavaTokenParsers

/**
 *  <b-expression>::= <b-term> [<orop> <b-term>]*
 *  <b-term>      ::= <not-factor> [AND <not-factor>]*
 *  <not-factor>  ::= [NOT] <b-factor>
 *  <b-factor>    ::= <b-literal> | <b-variable> | (<b-expression>)
 */

case class LogicalExpression(RelList:List[RelItem], tempRDD: List[Map[String,String]]) extends JavaTokenParsers {
  private lazy val b_expression: Parser[Boolean] = b_term ~ rep("||" ~ b_term) ^^ { case f1 ~ fs ⇒ (f1 /: fs)(_ || _._2) }
  private lazy val b_term: Parser[Boolean] = (b_not_factor ~ rep("&&" ~ b_not_factor)) ^^ { case f1 ~ fs ⇒ (f1 /: fs)(_ && _._2) }
  private lazy val b_not_factor: Parser[Boolean] = opt("!") ~ b_factor ^^ (x ⇒ x match { case Some(v) ~ f ⇒ !f; case None ~ f ⇒ f })
  private lazy val b_factor: Parser[Boolean] = b_literal | ("(" ~ b_expression ~ ")" ^^ { case "(" ~ exp ~ ")" ⇒ exp })
  private lazy val b_literal: Parser[Boolean] = "true" ^^ (x ⇒ true) | "false" ^^ (x ⇒ false) | ( b_item ~ opt("->" ~ b_item) ^^ {
    case key ~ Some("->" ~ value) ⇒ isTarget(key, value,tempRDD)
  } )

  // This will construct the list of variables for th
  // is parser
  //private lazy val b_variable: Parser[Boolean] = variableMap.keysIterator.map(Parser(_)).reduceLeft(_ | _) ^^ (x ⇒ variableMap(x))
  //private lazy val b_item: Parser[String] = variableMap.keysIterator.map(Parser(_)).reduceLeft(_ | _) ^^ (x ⇒ x)
  private lazy val b_item: Parser[String] = stringLiteral ^^ (x ⇒ x.substring(1, x.length() - 1))

  def isTarget(a: String, b: String, tempRDD: List[Map[String,String]]) = {
    /*val conf2 = new SparkConf()
    conf2.setAppName("loadTemp")
    conf2.setMaster("spark://hadoop-slave1:7077")
    val sc2 = new SparkContext(conf2)*/
    /*val nottemplatFilePath: String ="hdfs://hadoop:8020/spark/ccmicky/Temp/not"
    val not = sc.textFile(nottemplatFilePath).map(p => p.trim)
    val bcnot = sc.broadcast(not.collect)*/
    /*val templateFilePath : String = "hdfs://hadoop:8020/spark/ccmicky/Temp/"
    val valuetemplatFilePath: String ="hdfs://hadoop:8020/spark/ccmicky/Temp/"+a
    val keytemplatFilePath: String ="hdfs://hadoop:8020/spark/ccmicky/Temp/"+b
    var bckeys = List(b).toArray
    var bcvalues = List(a).toArray
    if (ClassifyTest.searchFold(templateFilePath,a))
      {
        val keys = sc2.textFile(keytemplatFilePath).map(p => p.trim)
        bckeys = sc2.broadcast(keys.collect).value
      }
    if (ClassifyTest.searchFold(templateFilePath,b))
    {
      val values = sc2.textFile(valuetemplatFilePath).map(p => p.trim)
        bcvalues = sc2.broadcast(values.collect).value
    }*/

    //val bckeys = List("不错").toArray

   // val bcvalues = List("位置").toArray
    val values = tempRDD.filter(p=> p.keys.head==a)
    var bcvalues = List(a).toArray
    if(values.length>0)
      {
        bcvalues = values.map{p=>
          p.values.head}.toArray
      }
   //bcvalues.foreach(p=>println("$$$$$$$$$$$$$$$$$$$$$$$$$$$&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"+p))
    val keys = tempRDD.filter(p=> p.keys.head==b)
    var bckeys = List(b).toArray
    if(keys.length>0)
      {
        bckeys= keys.map(p=>p.values.head).toArray
      }
    BFSearch.isTagedbyKey(bcvalues, bckeys,RelList) || BFSearch.isTagedbyKey(bckeys, bcvalues,RelList)
  }
  def sparse(expression: String) = this.parseAll(b_expression, expression)
}

object LogicalExpression1 {

  def sparse(hrRDD: RDD[ShortSentence], tempRDD: List[Map[String,String]])(value: String) : RDD[ShortSentence] ={
    //println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"+value)

    val filterhrRDD = hrRDD.filter{p =>
      val logicalexpression = new LogicalExpression(p.RelList,tempRDD)
      logicalexpression.sparse(value).get}
    filterhrRDD
  }
}

