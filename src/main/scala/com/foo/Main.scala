package com.foo

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl._
import com.foo.scada._
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.{DateTimeZone, DateTime}

import squants.time.Minutes
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


object Main extends App with StrictLogging {

  implicit val system = ActorSystem("streams-poc")
  implicit val materializer = FlowMaterializer()

  val startTime = new DateTime()
  val startDate = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeZone.forID("US/Eastern"))
  val endDate = new DateTime(2015, 1, 2, 0, 0, 0, DateTimeZone.forID("US/Eastern"))

  val endPointsSource = Source(generateData("1103", startDate, endDate) ++ generateData("1102", startDate, endDate) ++ generateData("1103", startDate, endDate))

  case class GraphPoint(meterGroupId:String, timeStamp:Timestamp, usageRate:BigDecimal, usage:BigDecimal)

  endPointsSource.groupBy(td ⇒ td.timestamp).map {
    case (timeStamp, trendStream) =>

      trendStream.fold(GraphPoint("", 0, 0.0, 0.0)) {
        case (accum, td) ⇒
         accum.copy(
           meterGroupId = "meterGroupOne",
           timeStamp = td.timestamp,
           usageRate = accum.usageRate + td.usageRate,
           usage = accum.usage + td.usage
         )
      }

  }.runWith(
    Sink.foreach(v ⇒
      v.onComplete{
        case Success(v) ⇒ println("Aggregated " + v)
        case Failure(err) ⇒ println(">>> Failed with " + err)
      }
    )
  )

  def generateData(endPoint: PointId, startTime: DateTime, endTime: DateTime) = {
    val ret = (0 to 1440) map { minute =>

      val durations = Set(
        DurationEntry(Minutes(5), 95.5, WeightedAverage),
        DurationEntry(Minutes(10), 90.5, WeightedAverage),
        DurationEntry(Minutes(15), 89.9, WeightedAverage)
      )

      val min = startTime.plusMinutes(minute)
      TrendData(endPoint, min.getMillis, Good, durations, 50.0, 200.0)
    }
    ret
  }
}






