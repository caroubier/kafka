package org.esgi.project


import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, _}
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.stream.ActorMaterializer

import com.typesafe.config.{Config, ConfigFactory}
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport

import org.apache.kafka.streams.kstream.{KGroupedStream => _, KStream => _, KTable => _, _}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import org.esgi.project.models.{CountView, Like, Response, View}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsValue, Json}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor
import io.github.azhur.kafkaserdeplayjson.{PlayJsonSupport => azhurPlayJsonSupport}

object Main extends PlayJsonSupport with azhurPlayJsonSupport {
  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer.create(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  // Configure Kafka Streams

  val props: Properties = {
    val p = new Properties()
    // name of the streaming application (aka project name)
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kazaa-streaming-app")
    // nom du broker
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  // randomize store names
  val randomUuid = UUID.randomUUID.toString
  val thirtySecondsStoreName = s"thirtySecondsVisitsStore-$randomUuid"
  val oneMinuteStoreName = s"oneMinuteVisitsStore-$randomUuid"
  val fiveMinutesStoreName = s"fiveMinuteVisitsStore-$randomUuid"

  // Run streams
  val streams: KafkaStreams = new KafkaStreams(buildProcessingGraph, props)
  streams.start()

  def buildProcessingGraph: Topology = {
    import Serdes._

    val builder: StreamsBuilder = new StreamsBuilder

    // Stream : unbonded sequence of structured data (= event facts or messages)
    // KStream : class creating an abstraction over a stream
    // used for a stream of records
    val likesStream: KStream[String, JsValue] = builder.stream[String, String]("likes")
      .mapValues(value => Json.parse(value))

    val viewsStream: KStream[String, JsValue] = builder.stream[String, String]("views")
      .mapValues(value => Json.parse(value))

    // TODO: Count views per category (second part of the URL)
    val groupedByFilm : KGroupedStream[Int, JsValue] = viewsStream.map { (_, view) =>
      val parsedView = view.as[View]
      (parsedView._id, view)
    }
      .groupByKey(Serialized.`with`(Serdes.Integer, PlaySerdes.create))

    val thirtySecondsWindowedViews = groupedByFilm.windowedBy(TimeWindows.of(30.seconds.toMillis).advanceBy(1.seconds.toMillis))
    val oneMinuteWindowedViews = groupedByFilm.windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(1.seconds.toMillis))
    val fiveMinutesWindowedViews = groupedByFilm.windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.seconds.toMillis))

    val thirtySecondsTableViews : KTable[Windowed[Int], CountView] = thirtySecondsWindowedViews.aggregate(CountView(0, 0, 0, 0))((key, value, currentView) =>
      value.asOpt[View].get.view_category match {
        case "start_only" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_start = currentView.count_start + 1
        )
        case "half" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_half = currentView.count_half + 1
        )
        case "full" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_full = currentView.count_full + 1
        )
      }
    )(Materialized.as(thirtySecondsStoreName).withValueSerde(toSerde))

    val oneMinuteTableViews : KTable[Windowed[Int], CountView] = oneMinuteWindowedViews.aggregate(CountView(0, 0, 0, 0))((key, value, currentView) =>
      value.asOpt[View].get.view_category match {
        case "start_only" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_start = currentView.count_start + 1
        )
        case "half" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_half = currentView.count_half + 1
        )
        case "full" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_full = currentView.count_full + 1
        )
      }
    )(Materialized.as(oneMinuteStoreName).withValueSerde(toSerde))

    val fiveMinutesTableViews : KTable[Windowed[Int], CountView] = fiveMinutesWindowedViews.aggregate(CountView(0, 0, 0, 0))((key, value, currentView) =>
      value.asOpt[View].get.view_category match {
        case "start_only" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_start = currentView.count_start + 1
        )
        case "half" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_half = currentView.count_half + 1
        )
        case "full" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_full = currentView.count_full + 1
        )
      }
    )(Materialized.as(fiveMinutesStoreName).withValueSerde(toSerde))

    builder.build()
  }


    def routes(): Route = {
      concat(
        path("movies" / Segment) {
          (id: String) =>
            get { context: RequestContext =>
              context.complete(
                Response(id = id, message = s"Hi, here's your id: $id")
              )
            }
        },
        path("some" / "other" / "route") {
          get {
            complete {
              Response(id = "foo", message = "Another silly message")
            }
          }
        }
      )
    }

    def main(args: Array[String]) {
      Http().bindAndHandle(routes(), "0.0.0.0", 8080)
      logger.info(s"App started on 8080")
    }
}
