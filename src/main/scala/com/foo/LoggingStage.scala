package com.foo

import akka.stream.stage.{TerminationDirective, Directive, Context, PushStage}
import com.typesafe.scalalogging.StrictLogging

/**
 * Created by al on 2/14/15.
 */
class LoggingStage[T] extends PushStage[T, T] with StrictLogging {

  override def onPush(elem: T, ctx: Context[T]): Directive = {
    //log.debug("Element flowing through: {}", elem)
    println(s"========================== Pushing ${elem}")
    ctx.push(elem)
  }

  override def onUpstreamFailure(cause: Throwable,
                                 ctx: Context[T]): TerminationDirective = {
    //log.error(cause, "Upstream failed.")
    println(s"========================== Failed ${cause.getMessage}")

    super.onUpstreamFailure(cause, ctx)
  }

  override def onUpstreamFinish(ctx: Context[T]): TerminationDirective = {
    //log.debug("Upstream finished")
    println(s"========================== Finished")

    super.onUpstreamFinish(ctx)
  }
}
