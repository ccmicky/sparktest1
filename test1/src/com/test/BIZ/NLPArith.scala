package com.test.BIZ


import com.test.Entity.ShortSentence
import spire.std.boolean

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Created by Administrator on 2015/11/15.
 */

abstract class Expr
case class Variable(name: String) extends Expr
case class Variable2(name: String, name2:String) extends Expr
case class Number(value: String) extends Expr
case class UnaryOp(op: String, expr: Expr) extends Expr
case class BinaryOp(op: String, left: Expr, right: Expr) extends Expr

object NLPArith {

  def parse(str: String) = Arith.parse(str)

  def evaluate(e: Expr, ss:ShortSentence): Boolean = simplify(e) match {

    case Variable(x) =>IncludeWord(x,ss)
    case Variable2(x1,x2) =>IncludeWord2(x1,x2,ss)
    case UnaryOp("!", x) =>  ! evaluate(x, ss)
    case UnaryOp("->", x) =>  evaluate(x, ss)
    case BinaryOp("||", x1, x2) =>evaluate(x1, ss) || evaluate(x2, ss)
    case BinaryOp("&&", x1, x2) =>evaluate(x1, ss)  && evaluate(x2, ss)


    //    case Number(x) => x
    //case UnaryOp("-", x) => "-" +evaluate(x, ss)
    // case BinaryOp("+", x1, x2) => (evaluate(x1, ss) + evaluate(x2, ss)) + ss
    //    case BinaryOp("-", x1, x2) => (evaluate(x1, ss) + "_" + evaluate(x2, ss)) + ss
    // case BinaryOp("**", x1, x2) => (evaluate(x1, ss) +"*"+ evaluate(x2, ss)) + ss
    //    case BinaryOp("/", x1, x2) => (evaluate(x1, ss) + "/" + evaluate(x2, ss)) + ss

  }

  // ->
  def IncludeWord(wordlist:String, ss:ShortSentence):Boolean=
  {
    return false
     val wl = GenRuleWordList(wordlist)
    for( i <- ss.RelList)
    {
      if( wl.contains(i.Word1)|| wl.contains(i.Word2))
        {
          return true
        }
    }
    false
  }

  def FormatRuleWord(w:String):String={ w.replace("\"","")}
  def GenRuleWordList(w:String): Array[String]={FormatRuleWord(w).split(",")}

  def IncludeWord2(wordlist1:String,wordlist2:String, ss:ShortSentence):Boolean=
  {
    return false
    val wl1 = GenRuleWordList(wordlist1)
    val wl2 =  GenRuleWordList(wordlist2)
    for( i <- ss.RelList)
    {
      if( wl2.contains(i.Word1)&& wl1.contains(i.Word2))
      {
        return true
      }
    }
    false
  }


  /*
     * Lex's version:
     */
  def simplify(e: Expr): Expr = {
    // first simplify the subexpressions
    val simpSubs = e match {
      // Ask each side to simplify
      case BinaryOp(op, left, right) => BinaryOp(op, simplify(left), simplify(right))
      // Ask the operand to simplify
      case UnaryOp(op, operand) => UnaryOp(op, simplify(operand))
      // Anything else doesn't have complexity (no operands to simplify)
      case _ => e
    }

    // now simplify at the top, assuming the components are already simplified
    def simplifyTop(x: Expr) :Expr = {
      x match {
        // Double negation returns the original value
        case UnaryOp("-", UnaryOp("-", x)) => x

        // Positive returns the original value
        case UnaryOp("+", x) => x

        // Multiplying x by 1 returns the original value
        case BinaryOp("**", x, Number("1")) => x

        // Multiplying 1 by x returns the original value
        case BinaryOp("**", Number("1"), x) => x

        // Multiplying x by 0 returns zero
        case BinaryOp("**", x, Number("0")) => Number("0")

        // Multiplying 0 by x returns zero
        case BinaryOp("**", Number("0"), x) => Number("0")

        // Dividing x by 1 returns the original value
        case BinaryOp("/", x, Number("1")) => x

        // Dividing x by x returns 1
        case BinaryOp("/", x1, x2) if x1 == x2 => Number("1")

        // Adding x to 0 returns the original value
        case BinaryOp("+", x, Number("0")) => x

        // Adding 0 to x returns the original value
        case BinaryOp("+", Number("0"), x) => x

        // Anything else cannot (yet) be simplified
        case e => e
      }
    }

    simplifyTop(simpSubs)
  }
}

object Arith extends JavaTokenParsers {
  def compose(bi: Expr, list: List[String ~ Expr]): Expr = list match {
    case Nil => {
      bi}
    case (op ~ e) :: t =>{
      compose(BinaryOp(op, bi, e), t)
    }
  }

  def decompose(list: Option[String] ~ Expr): Expr = list match {
    case Some(v) ~ f =>{
      UnaryOp("!", f)}
    case None ~ f =>{f}
  }

  def expr: Parser[Expr] = term ~ rep("||" ~ term) ^^ {
    case t ~ list => {
      compose(t, list)}
  }

  def term: Parser[Expr] = nofactor ~ rep( "&&" ~ nofactor ) ^^ {
    case t ~ list => compose(t, list)
  }

  def nofactor : Parser[Expr] =  opt("!") ~ factor ^^ (x =>
     decompose(x))

  def factor: Parser[Expr] = stringLiteral ~ "->" ~ stringLiteral ^^{
     x =>
       UnaryOp("->", Variable2(x._1._1,x._2))
  }  |stringLiteral ^^ { x => Variable(x) } |  "(" ~> expr <~ ")"


  def parse(str: String): Expr = {
    val result = parseAll(expr, str)
    if (result.successful)
      {
        println(NLPArith.evaluate(result.get,null))
        result.get
      }

    else Number("0")
  }

}
