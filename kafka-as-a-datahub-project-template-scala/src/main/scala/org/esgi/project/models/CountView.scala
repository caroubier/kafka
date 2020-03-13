package org.esgi.project.models

import play.api.libs.json.{Json, OFormat}

case class CountView (
  count_start : Int,
  count_half : Int,
  count_full : Int,
  count_element : Int,
  title : Option[String] = Option("Unknown title")
  )

  object CountView {
    implicit val format: OFormat[CountView] = Json.format[CountView]
  }