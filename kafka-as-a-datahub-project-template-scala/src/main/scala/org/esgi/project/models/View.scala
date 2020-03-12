package org.esgi.project.models

import play.api.libs.json.{Json, OFormat}

case class View(
                   _id : Int,
                   title : String,
                   view_category : String,
                 )

object View {
  implicit val format: OFormat[View] = Json.format[View]
}