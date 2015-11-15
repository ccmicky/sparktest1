package com.test.BIZ


import org.apache.spark.SparkContext

import scala.util.parsing.combinator.JavaTokenParsers
/**
 * Created by Administrator on 2015/11/13.
 */
case class LogicalExpression (sc:SparkContext, list:List[String]) extends JavaTokenParsers {
  private lazy val b_expression: Parser[Boolean] = b_term ~ rep("||" ~ b_term) ^^ { case f1 ~ fs => (f1 /: fs)(_ || _._2) }
  private lazy val b_term: Parser[Boolean] = (b_not_factor ~ rep("&&" ~ b_not_factor)) ^^ { case f1 ~ fs => (f1 /: fs)(_ && _._2) }
  private lazy val b_not_factor: Parser[Boolean] = opt("!") ~ b_factor ^^ (x => x match { case Some(v) ~ f => !f; case None ~ f => f })
  private lazy val b_factor: Parser[Boolean] = b_literal | ("(" ~ b_expression ~ ")" ^^ { case "(" ~ exp ~ ")" => exp })
  private lazy val b_literal: Parser[Boolean] = "true" ^^ (x => true) | "false" ^^ (x => false) | ( b_item ~ opt("->" ~ b_item) ^^ {
    case key ~ Some("->" ~ value) => isTarget(key, value)
  } )


  def isTarget(a: String, b: String): Boolean  =
  {
    println(a + "->" + b)
    return list.contains(a) && list.contains(b)
  }

  // This will construct the list of variables for th
  // is parser
  //private lazy val b_variable: Parser[Boolean] = variableMap.keysIterator.map(Parser(_)).reduceLeft(_ | _) ^^ (x ? variableMap(x))
  //private lazy val b_item: Parser[String] = variableMap.keysIterator.map(Parser(_)).reduceLeft(_ | _) ^^ (x ? x)
  private lazy val b_item: Parser[String] = stringLiteral ^^ (x => x.substring(1, x.length() - 1))

  def sparse(expression: String) = this.parseAll(b_expression, expression)
}
