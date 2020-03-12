package org.esgi.project.models

import play.api.libs.json.{Json, OFormat}

case class Like (
                  _id : Int,
                  score : Float
                )

object Like {
  implicit val format : OFormat[Like] = Json.format[Like]
}