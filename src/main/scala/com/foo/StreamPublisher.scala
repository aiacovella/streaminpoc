package com.foo

import akka.actor.{ActorLogging, Props}
import akka.stream.actor.ActorPublisher

import scala.annotation.tailrec

object StreamPublisher {
  def props: Props = Props[StreamPublisher]

  case object JobAccepted
  case object JobDenied
}

class StreamPublisher extends ActorPublisher[Payload] with ActorLogging {
  import akka.stream.actor.ActorPublisherMessage._
  import StreamPublisher._

  val MaxBufferSize = 100
  var buf = Vector.empty[Payload]


  override def onComplete(): Unit = {
println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    super.onComplete()
  }

  def receive = {
      //    case job: Job if buf.size == MaxBufferSize =>
      //println(">>>> Denying Job " + job.payload)
      //      sender() ! JobDenied
    case job: Job =>
      sender() ! JobAccepted

log.error(">>>>>>>>> Publisher got " + job.endPoint)
      
//log.error(">>>> Total Demand: " + totalDemand)
      if (buf.isEmpty && totalDemand > 0) {
        val msg = Payload(job.endPoint, self)
//log.error(">>>> Pushing Job " + job.ticker)
        onNext(msg)
      }
      else {
//log.error(">>>> Buffering Job " + job.ticker)

        buf :+= Payload(job.endPoint, self)
        deliverBuf()
      }
    case Request(_) =>
      // Stream subscriber requesting more elements
      deliverBuf()
    case Cancel =>
      // this is when a subscription is cancelled
      context.stop(self)
  }

  @tailrec final def deliverBuf(): Unit =
    if (totalDemand > 0) {
      /*
       * totalDemand is a Long and could be larger than
       * what buf.splitAt can accept
       */
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use foreach onNext
      } else {
        val (use, keep) = buf.splitAt(Int.MaxValue)
        buf = keep
        use foreach onNext
        deliverBuf()
      }
    }
}
