package com.foo

import akka.actor.ActorRef
import org.joda.time.DateTime

case class Payload(stockTicker: String, replyTo:ActorRef)

case class Result(e: Either[Retry, StockResult])

case class Retry(stockTicker: String, replyTo:ActorRef)

case class StockResult(stockTicker: String, price: BigDecimal, replyTo:ActorRef)

case class Job(endPoint: PointId, startDate: DateTime, endDate: DateTime)

