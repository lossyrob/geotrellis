package geotrellis.spark

import spray.json._
import spray.json.DefaultJsonProtocol._

case class KeyBounds[K](
  minKey: K,
  maxKey: K
)

object KeyBounds {
  implicit class KeyBoundsSeqMethods[K: Boundable](seq: Seq[KeyBounds[K]]){
    def includeKey(key: K): Boolean = {
      val boundable = implicitly[Boundable[K]]
      seq
        .map{ kb => boundable.includes(key, kb) }
        .foldLeft(false)(_ || _)    
    }
  }

  implicit def keyBoundsToTuple[K](keyBounds: KeyBounds[K]): (K, K) = (keyBounds.minKey, keyBounds.maxKey)

  implicit def keyBoundsFormat[K: JsonFormat]: RootJsonFormat[KeyBounds[K]] =
    new RootJsonFormat[KeyBounds[K]] {

      def write(keyBounds: KeyBounds[K]) =
        JsObject(
          "minKey" -> keyBounds.minKey.toJson,
          "maxKey" -> keyBounds.maxKey.toJson
        )

      def read(value: JsValue): KeyBounds[K] =
        value.asJsObject.getFields("minKey", "maxKey") match {
          case Seq(minKey, maxKey) =>
            KeyBounds(minKey.convertTo[K], maxKey.convertTo[K])
          case _ =>
            throw new DeserializationException("${classOf[KeyBounds[K]] expected")
        }
    }
}