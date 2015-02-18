package com.foo

import akka.actor.{Props, ActorSystem}
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.{TerminationDirective, Directive, Context, PushStage}
import com.foo.scada._
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.{Days, DateTimeZone, DateTime}

import scala.concurrent.Future

import squants.time.{Minutes, Time}
import squants.energy.{KilowattHours, Kilowatts}


/**
 * Created by al on 2/14/15.
 */
object Main extends App with StrictLogging {

  implicit val system = ActorSystem("streams-poc")

  implicit val materializer = FlowMaterializer()

  //val jobManager = system.actorOf(Props[StreamPublisher], "jobmanager")
  //  val jobManagerSource = Source[Payload](StreamPublisher.props)

  val startTime = new DateTime()

  val startDate = new DateTime(2014, 10, 10, 0, 0, 0, DateTimeZone.forID("US/Eastern"))

  val sourceData = Source(splitIntoDays(Job(s"ENDPOINT-2", startDate, null)))

  import scala.concurrent.ExecutionContext.Implicits.global

  println("START: " + new DateTime())

  val materializedMap = sourceData
    .mapAsync{endPointDay =>
      Future {
        hisoricalData(endPointDay)
      }
    }.mapConcat(identity)             // convert stream element into a sequence of elements then flatten
    .transform(() =>  new AveragingStage())
    .foreach{trendData =>
      //val fiveMinuteAverage = trendData.data.find(dt => dt.durationType == RollingAverage && dt.duration == Minutes(60))
      // println(s"Endpoint: ${trendData.pointId} Time: ${new DateTime(trendData.timestamp)} Power:${trendData.power.toKilowatts}} 5 Min Avg: ${fiveMinuteAverage}")
      // println(s"product: ${new DateTime(p.timestamp, DateTimeZone.UTC)} power: ${p.power}")
    }
    .onComplete { f =>
      println("DONE and DONE")

     println(s">>>>>>>>>>>>> Job took ${(new DateTime().getMillis - startTime.getMillis) / 1000} seconds to run")

      system.shutdown()
  }



  println("End: " + new DateTime())

//    .map{v =>
//        splitIntoDays(v)
//    }.mapConcat(identity)
//    .foreach{p =>
//        println(s"\nproduct: $p")
//    }

//    .transform(() => new LoggingStage[Result])
//    .to(Sink(StreamSubscriber.props)).run



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

    println(s">>>>>>>>>>>>>>>>>> Retrieve start ${endPointDay.startTime}")
    //Thread.sleep(1000)

    val ret = (0 to 1440) map {minute =>

      val durations = Set(
        DurationEntry(Minutes(5), 95.5, WeightedAverage),
        DurationEntry(Minutes(10), 90.5, WeightedAverage),
        DurationEntry(Minutes(15), 89.9, WeightedAverage)
      )

      val min = endPointDay.startTime.plusMinutes(minute)
      TrendData(endPointDay.endPoint, min.getMillis, Good, durations, Kilowatts(minute), KilowattHours(minute *2))
    }
    println(s"***************** Retrieve end ${endPointDay.startTime}")

    ret
  }

}

class AveragingStage extends PushStage[TrendData, TrendData] with TimeSeriesCalculator with StrictLogging {
  implicit def orderingByEpocTime[A <: TimeSeriesEntry]: Ordering[A] = Ordering.by(e ⇒ e.epoc)

  val queue:FiniteQueue[TimeSeriesEntry] = new FiniteQueue[TimeSeriesEntry](60)

//  val array: Array[TimeSeriesEntry] = Array()

  override def onPush(trendData: TrendData, ctx: Context[TrendData]): Directive = {
//    println(s"Averaging Element : ${trendData}")

    queue.enqueue(TimeSeriesEntry(trendData.timestamp, trendData.power.toKilowatts, trendData.quality))

//    implicit def orderingByEpocTime[A <: TimeSeriesEntry]: Ordering[A] = Ordering.by(e ⇒ e.epoc)


    val timeSeries = collection.SortedSet(queue.toSeq: _*)

    val rollingAverages = Set(
      DurationEntry(Minutes(5), calculate(5, timeSeries), RollingAverage),
      DurationEntry(Minutes(15), calculate(15, timeSeries), RollingAverage),
      DurationEntry(Minutes(30), calculate(30, timeSeries), RollingAverage),
      DurationEntry(Minutes(60), calculate(60, timeSeries), RollingAverage))
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


/**
 * Implementation of a Queue with restricted size.
 * @param limit Size limit of the queue. If more elements are pushed into the queue, the oldest will be removed first if size will be exceeded
 * @tparam T Type parameter for elements to be stored in queue
 */
class FiniteQueue[T](limit: Int) extends scala.collection.mutable.Queue[T] {
  override def enqueue(elems: T*) {
    elems.foreach { elem ⇒
      if (size >= limit) super.dequeue()
      this += elem
    }
  }
}