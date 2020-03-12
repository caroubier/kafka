package org.esgi.project

import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json.{JsValue, Json}

object PlaySerdes {
  def create = {
    Serdes.fromFn[JsValue](
      (value: JsValue) => Json.stringify(value).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray))
    )
  }
}
