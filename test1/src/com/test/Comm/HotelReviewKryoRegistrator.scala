package com.test.Comm

import com.esotericsoftware.kryo.Kryo
import com.test.Entity.{RelItem, ShortSentence}
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by clcai on 2015/10/27.
 */
class HotelReviewKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[ShortSentence])
    kryo.register(classOf[RelItem])
  }
}