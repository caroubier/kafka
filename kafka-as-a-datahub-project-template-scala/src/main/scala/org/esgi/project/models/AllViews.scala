package org.esgi.project.models

import play.api.libs.json.Json

case class AllViews (
                      _id: Int,
                      views: Long
                    )

object AllViews {
  implicit val format = Json.format[AllViews]
}