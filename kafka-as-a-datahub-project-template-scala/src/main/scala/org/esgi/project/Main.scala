package org.esgi.project


import java.time.Instant
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
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}
import org.esgi.project.models.{AllViews, CountView, Like, Response, View}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor
import io.github.azhur.kafkaserdeplayjson.{PlayJsonSupport => azhurPlayJsonSupport}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.esgi.project.models.CountMovie.{Stats, TimeView}

import scala.util.Try

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
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kazaa-streaming-app-BEDROPOI")
    // nom du broker
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  // randomize store names
  val randomUuid = UUID.randomUUID.toString
  val moviesViewCountStoreName = s"moviesViewCountStore-$randomUuid"
  val PastStoreName = s"PastVisitsStore-$randomUuid"
  val oneMinStoreName = s"oneMinVisitsStore-$randomUuid"
  val fiveMinStoreName = s"fiveMinVisitsStore-$randomUuid"

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


    // TODO: Top 10 by views

    val groupedById: KGroupedStream[Int, JsValue] = viewsStream
      .map { (_, view) =>
        val parsedView = view.as[View]
        (parsedView._id, view)
      }
      .groupByKey(Serialized.`with`(Serdes.Integer, PlaySerdes.create))
    //TOP TEN
    val groupedByIdWithCount: KTable[Int, Long] = groupedById
      .count()(Materialized.as(moviesViewCountStoreName).withValueSerde(Serdes.Long))


    // TODO: number of views by movie
    val groupedByFilm: KGroupedStream[Int, JsValue] = viewsStream.map { (_, view) =>
      val parsedView = view.as[View]
      (parsedView._id, view)
    }
      .groupByKey(Serialized.`with`(Serdes.Integer, PlaySerdes.create))

    val oneMinWindowedViews = groupedByFilm.windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(1.seconds.toMillis))
    val fiveMinWindowedViews = groupedByFilm.windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.seconds.toMillis))

    val PastTableViews: KTable[Int, CountView] = groupedByFilm.aggregate(CountView(0, 0, 0, 0))((key, value, currentView) =>
      value.asOpt[View].get.view_category match {
        case "start_only" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_start = currentView.count_start + 1,
          title = Option(value.asOpt[View].get.title)

        )
        case "half" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_half = currentView.count_half + 1,
          title = Option(value.asOpt[View].get.title)

        )
        case "full" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_full = currentView.count_full + 1,
          title = Option(value.asOpt[View].get.title)

        )
      }
    )(Materialized.as(PastStoreName).withValueSerde(toSerde))

    val oneMinTableViews: KTable[Windowed[Int], CountView] = oneMinWindowedViews.aggregate(CountView(0, 0, 0, 0))((key, value, currentView) =>
      value.asOpt[View].get.view_category match {
        case "start_only" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_start = currentView.count_start + 1,
          title = Option(value.asOpt[View].get.title)

        )
        case "half" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_half = currentView.count_half + 1,
          title = Option(value.asOpt[View].get.title)

        )
        case "full" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_full = currentView.count_full + 1,
          title = Option(value.asOpt[View].get.title)

        )
      }
    )(Materialized.as(oneMinStoreName).withValueSerde(toSerde))

    val fiveMinTableViews: KTable[Windowed[Int], CountView] = fiveMinWindowedViews.aggregate(CountView(0, 0, 0, 0))((key, value, currentView) =>
      value.asOpt[View].get.view_category match {
        case "start_only" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_start = currentView.count_start + 1,
          title = Option(value.asOpt[View].get.title)

        )
        case "half" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_half = currentView.count_half + 1,
          title = Option(value.asOpt[View].get.title)

        )
        case "full" => currentView.copy(
          count_element = currentView.count_element + 1,
          count_full = currentView.count_full + 1,
          title = Option(value.asOpt[View].get.title)

        )
      }
    )(Materialized.as(fiveMinStoreName).withValueSerde(toSerde))


    builder.build()
  }


  def routes(): Route = concat(
    path("movies" / IntNumber) { id: Int =>
      get {
        import scala.collection.JavaConverters._
        val kvStorePast: ReadOnlyKeyValueStore[Int, CountView] = streams.store(PastStoreName, QueryableStoreTypes.keyValueStore[Int, CountView]())
        val availableKeysPast = kvStorePast.get(id)

        val kvStore1Min: ReadOnlyWindowStore[Int, CountView] = streams.store(oneMinStoreName, QueryableStoreTypes.windowStore[Int, CountView]())
        val toTimeOneMin = Instant.now().toEpochMilli
        val fromTimeOneMin = toTimeOneMin - (60 * 1000)
        val availableKeys1Min = kvStore1Min.fetch(id, fromTimeOneMin, toTimeOneMin).asScala.toList.last.value

        val kvStore5Min: ReadOnlyWindowStore[Int, CountView] = streams.store(fiveMinStoreName, QueryableStoreTypes.windowStore[Int, CountView]())
        val toTimeFiveMin = Instant.now().toEpochMilli
        val fromTimeFiveMin = toTimeFiveMin - (300 * 1000)
        val availableKeys5Min = kvStore5Min.fetch(id, fromTimeFiveMin, toTimeFiveMin).asScala.toList.last.value


        val stats: Stats = Stats(

          TimeView(
            start_only = Try(availableKeysPast.count_start).getOrElse(0),
            half = Try(availableKeysPast.count_half).getOrElse(0),
            full = Try(availableKeysPast.count_full).getOrElse(0)
          ),
          TimeView(
            start_only = Try(availableKeys1Min.count_start).getOrElse(0),
            half = Try(availableKeys1Min.count_half).getOrElse(0),
            full = Try(availableKeys1Min.count_full).getOrElse(0)
          ),
          TimeView(
            start_only = Try(availableKeys5Min.count_start).getOrElse(0),
            half = Try(availableKeys5Min.count_half).getOrElse(0),
            full = Try(availableKeys5Min.count_full).getOrElse(0)
          )


        )

        complete(
          stats
        )
      }

    },
    pathPrefix("stats") {
      pathPrefix("ten") {
        concat(
          pathPrefix("best") {
            concat(
              path("score") {
                get {
                  complete("best_score")
                }
              },
              path("views") {
                get {
                  import scala.collection.JavaConverters._
                  val bestViews: ReadOnlyKeyValueStore[Int, Long] = streams.store(moviesViewCountStoreName, QueryableStoreTypes.keyValueStore[Int, Long]())
                  val list: List[KeyValue[Int, Long]] = bestViews.all().asScala.toList.sortBy(_.value)(Ordering.Long.reverse).take(10)
                  complete(for (v <- list) yield AllViews(v.key, v.value))
                }
              }
            )
          },
          pathPrefix("worst") {
            concat(
              path("score") {
                get {
                  complete("worst_score")
                }
              },
              path("views") {
                get {
                  import scala.collection.JavaConverters._
                  val bestViews: ReadOnlyKeyValueStore[Int, Long] = streams.store(moviesViewCountStoreName, QueryableStoreTypes.keyValueStore[Int, Long]())
                  val list: List[KeyValue[Int, Long]] = bestViews.all().asScala.toList.sortBy(_.value)(Ordering.Long).take(10)
                  complete(for (v <- list) yield AllViews(v.key, v.value))
                }
              }
            )
          }
        )
      }
    }
  )


  def main(args: Array[String]) {
    Http().bindAndHandle(routes(), "0.0.0.0", 8080)
    logger.info(s"App started on 8080")
  }
}
