package com.foo

import akka.actor.{ActorLogging, Actor, Props, ActorRef}
import akka.routing.{RoundRobinRoutingLogic, Router, ActorRefRoutee}
import akka.stream.actor.{MaxInFlightRequestStrategy, ActorSubscriber, ActorSubscriberMessage}

object StreamSubscriber {
  case class Msg(id: Int, replyTo: ActorRef)
  case class Work(ticker: String, price: BigDecimal)
  case class Reply(ticker: String)
  case class Done(id: Int)

  def props: Props = Props(new StreamSubscriber)
}

class StreamSubscriber extends ActorSubscriber with ActorLogging {
  import StreamSubscriber._
  import ActorSubscriberMessage._

  val MaxQueueSize = 5
  var queue = Map.empty[String, Payload]

  val router = {
//    val routees = Vector.fill(3) { 
//      ActorRefRoutee(context.actorOf(Props[Worker]))
//    }

    val routees = (0 to 3).map{cnt ⇒ ActorRefRoutee(context.actorOf(Props[Worker], s"update-worker-$cnt"))}
    Router(RoundRobinRoutingLogic(), routees)
  }

  override val requestStrategy = new MaxInFlightRequestStrategy(max = MaxQueueSize) {
    override def inFlightInternally: Int = queue.size
  }

  def receive = {
    case OnNext(StockResult(ticker, price, replyTo)) =>

    case OnNext(Result(either)) ⇒

      either match {
        case Left(r) ⇒
          // remove the failed one from the queue and resend message back to producer
          // TODO: this should be a key of pointid:startTime:endTime
          queue -= r.stockTicker

//          r.replyTo ! Job(r.stockTicker)
          
        case Right(sr) ⇒

//          println(s">>>>>>>>>  Subscriber got result for ticker: ${sr.stockTicker.toUpperCase()}")
//          queue += (sr.stockTicker -> Payload(sr.stockTicker, sr.replyTo))
//          assert(queue.size <= MaxQueueSize, s"queued too many: ${queue.size}")
//
//          router.route(Work(sr.stockTicker, sr.price), self)
      }


    case Reply(ticker) =>
       //     queue(id) ! Done(id)
      queue -= ticker

    case other => log.error(">>>>>>>>>  GOT OTHER: " + other)
  }
}

class Worker extends Actor {
  import StreamSubscriber._
  def receive = {
    case work @ Work(ticker, price) =>
      println(s">>>> Worker ${self.path} handling ${work}")
 
      // ...
      sender() ! Reply(ticker)
  }
}