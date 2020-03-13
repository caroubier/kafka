package org.esgi.project.models

import play.api.libs.json.{Json, OFormat}

object CountMovie {

  case class TimeView(
                       start_only: Int,
                       half: Int,
                       full: Int,
                     )

  case class CountMovie(
                         _id: Int,
                         title: String,
                         view_count: Int,
                         stats: Stats
                       )

  case class Stats(
                    PastView: TimeView,
                    OneMinView: TimeView,
                    FiveMinView: TimeView
                  )


  implicit val format: OFormat[TimeView] = Json.format[TimeView]
  implicit val format2: OFormat[CountMovie] = Json.format[CountMovie]
  implicit val format3: OFormat[Stats] = Json.format[Stats]


}