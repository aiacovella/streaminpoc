package com.foo

import akka.actor.{Props, ActorSystem}
import akka.stream.FlowMaterializer
import akka.stream.scaladsl._
import akka.stream.stage.{TerminationDirective, Directive, Context, PushStage}
import com.foo.scada._
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.{Seconds, Days, DateTimeZone, DateTime}

import scala.concurrent.Future

import squants.time.{Minutes, Time}
import squants.energy.{KilowattHours, Kilowatts}


/**
 * Created by al on 2/14/15.
 */
object Main extends App with StrictLogging {

  implicit val system = ActorSystem("streams-poc")

  implicit val materializer = FlowMaterializer()


  val startTime = new DateTime()

  val startDate = new DateTime(2014, 12, 1, 0, 0, 0, DateTimeZone.forID("US/Eastern"))

  val sourceDataEnpoint1 = Source(splitIntoDays(Job(s"ENDPOINT-1", startDate, null)))
  val sourceDataEnpoint2 = Source(splitIntoDays(Job(s"ENDPOINT-2", startDate, null)))

  val allSources = Source(splitIntoDays(Job(s"ENDPOINT-1", startDate, null)) ++ splitIntoDays(Job(s"ENDPOINT-2", startDate, null)))

  import scala.concurrent.ExecutionContext.Implicits.global

  val materializedMap1 = sourceDataEnpoint1
    .mapAsync{endPointDay =>
    Future {
      hisoricalData(endPointDay)
    }
  }.mapConcat(identity)             // convert stream element into a sequence of elements then flatten
    .transform(() =>  new AveragingStage())

  val materializedMap2 = sourceDataEnpoint2
    .mapAsync{endPointDay =>
      Future {
        hisoricalData(endPointDay)
      }
    }.mapConcat(identity)             // convert stream element into a sequence of elements then flatten
    .transform(() =>  new AveragingStage())

  val materializedMaps = Set(materializedMap1, materializedMap2)

  var endPointOneCount = 0
  var endPointTwoCount = 0

  val sink = ForeachSink[TrendData]{ v ⇒
    if (v.pointId == "ENDPOINT-2"){
      endPointTwoCount += 1
    }
    else {
      endPointOneCount += 1
    }
  }

  val materialized = FlowGraph { implicit builder =>
    import FlowGraphImplicits._
    val merge = Merge[TrendData]("MergedStreams")

    val merges = materializedMaps.map{ mm ⇒
      mm ~> merge
    }

    merges.last ~> sink
  }.run

  materialized.get(sink).onComplete{ s ⇒
    val seconds = Seconds.secondsBetween(startTime, new DateTime()).getSeconds
    println(s"Completed in $seconds seconds")

    println()
    system.shutdown()
  }



  //Thread.sleep(60000)
//  system.shutdown()



//  system.shutdown()

  def splitIntoDays(job: Job) = {
    val beginOfDayForStartTime = job.startDate.withMillisOfDay(0)
    val endOfDayToday = new DateTime(job.startDate.getZone).withTime(23, 59, 59, 999)
    val daysBetween = Days.daysBetween(beginOfDayForStartTime, endOfDayToday).getDays()

    (0 to daysBetween ).map{day =>
      val beginDay = beginOfDayForStartTime.plusDays(day)
      val endDay = beginDay.withTime(23, 59, 59, 999)
      EndpointDay(job.endPoint, beginDay, endDay)
    }

  }

  def hisoricalData(endPointDay: EndpointDay) = {
    // simulate historical data and response lag of 1 second

    val ret = (0 to 1440) map {minute =>

      val durations = Set(
        DurationEntry(Minutes(5), 95.5, WeightedAverage),
        DurationEntry(Minutes(10), 90.5, WeightedAverage),
        DurationEntry(Minutes(15), 89.9, WeightedAverage)
      )

      val min = endPointDay.startTime.plusMinutes(minute)
      TrendData(endPointDay.endPoint, min.getMillis, Good, durations, Kilowatts(minute), KilowattHours(minute *2))
    }
    ret
  }

}

class AveragingStage extends PushStage[TrendData, TrendData] with TimeSeriesCalculator with StrictLogging {
  implicit def orderingByEpocTime[A <: TimeSeriesEntry]: Ordering[A] = Ordering.by(e ⇒ e.epoc)

  val queue:FiniteQueue[TimeSeriesEntry] = new FiniteQueue[TimeSeriesEntry](60)

  override def onPush(trendData: TrendData, ctx: Context[TrendData]): Directive = {
    queue.enqueue(TimeSeriesEntry(trendData.timestamp, trendData.power.toKilowatts, trendData.quality))

    val rollingAverages = Set(
      DurationEntry(Minutes(5), calculate(5, queue), RollingAverage),
      DurationEntry(Minutes(15), calculate(15, queue), RollingAverage),
      DurationEntry(Minutes(30), calculate(30, queue), RollingAverage),
      DurationEntry(Minutes(60), calculate(60, queue), RollingAverage))
      ctx.push(trendData.copy(data = trendData.data ++ rollingAverages))

  }

  override def onUpstreamFailure(cause: Throwable, ctx: Context[TrendData]): TerminationDirective = {
    logger.error("Upstream failed.", cause)
    super.onUpstreamFailure(cause, ctx)
  }

  override def onUpstreamFinish(ctx: Context[TrendData]): TerminationDirective = {
    logger.debug("Upstream finished")
    super.onUpstreamFinish(ctx)
  }
}

