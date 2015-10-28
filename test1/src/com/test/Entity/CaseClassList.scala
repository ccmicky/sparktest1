package com.test.Entity

/**
 * Created by Administrator on 2015/10/22.
 */
case class KeyRelWordItem(Word:String,RelWord:String, RelWordPOS:String)
case class WordItem(Word:String, POS:String)
case class RelItem(Rel:String, Word1:String,w1POS:String, w1Index:Int, Word2:String, w2POS:String, w2Index:Int )
case class ShortSentence(idx:Long, hotelid:Int, writing:Int,RelList:List[RelItem]  )


case class HotelKeyWordRelWord(hotelid:Int, KeyWord:String, RelWord:String, RelWordPOS:String, ADV:String, ADVPOS:String, NO:String , NOPOS:String)

/*case class KeyRelWordItem(Word:String,RelWord:String, RelWordPOS:String)
case class WordItem(Word:String, POS:String)
case class RelItem(Rel:String, Word1:String,w1POS:String, w1Index:Int, Word2:String, w2POS:String, w2Index:Int )
 class ShortSentence extends java.io.Serializable ( idx:Long, hotelid:Int, writing:Int,RelList:List[RelItem]  )*/
