package com.foo.scada

import com.foo._
import squants.energy.{Energy, Power}
import squants.time.Time

sealed trait ReadingQuality
case object Good extends ReadingQuality
case object Bad extends ReadingQuality
case object Suspect extends ReadingQuality

object ReadingQuality {
  private val options = Set(Good, Bad, Suspect)

  def fromString(value: String): Option[ReadingQuality] = options.find(_.toString.equalsIgnoreCase(value))
}

sealed trait DurationType
case object WeightedAverage extends DurationType
case object RollingAverage extends DurationType

object DurationType {
  private val options = Set(WeightedAverage, RollingAverage)

  def fromString(value: String): Option[DurationType] = options.find(_.toString.equalsIgnoreCase(value))
}

case class TrendData(
  pointId: PointId,
  timestamp: Timestamp,
  quality: ReadingQuality,
  data: Set[DurationEntry],
  usageRate: BigDecimal,
  usage: BigDecimal,
  id: org.bson.types.ObjectId = new org.bson.types.ObjectId)


case class DurationEntry(duration: Time, value: BigDecimal, durationType: DurationType)
