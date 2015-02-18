/* Copyright ©2010-2015, Viridity Energy, Inc. All Rights Reserved.
 *
 * Viridity®, Viridity Energy®, and VPower™ are trademarks of Viridity Energy, Inc.  All other product
 * names, trade names, trademarks, and logos used in this documentation are the property of their
 * respective owners.  Use of any other company’s trademarks, trade names, product names and logos does
 * not constitute: (1) an endorsement by such other company of Viridity Energy, Inc. or its products or
 * services, or (2) an endorsement of by Viridity Energy, Inc. or such other company or its products or
 * services.
 *
 * This software includes code written by third parties, including Scala (Copyright ©2002-2013, EPFL,
 * Lausanne) and other code. Additional details regarding such third party code, including applicable
 * copyright, legal and licensing notices are available at:
 * http://support.viridityenergy.com/thirdpartycode.
 *
 * No part of this documentation may be reproduced, transmitted, or otherwise distributed in any form or
 * by any means (electronic or otherwise) without the prior written consent of Viridity Energy, Inc..
 * You may not use this documentation for any purpose except in connection with your properly licensed
 * use or evaluation of Viridity Energy, Inc. software.  Any other use, including but not limited to
 * reverse engineering such software or creating derivative works thereof, is prohibited.  If your
 * license to access and use the software that this documentation accompanies is terminated, you must
 * immediately return this documentation to Viridity Energy, Inc. and destroy all copies you may have.
 *
 * IN NO EVENT SHALL VIRIDITY ENERGY, INC. BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING BUT NOT LIMITED TO LOST PROFITS, ARISING OUT OF THE
 * USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF VIRIDITY ENERGY, INC. HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * VIRIDITY ENERGY, INC. SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE AND
 * ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED "AS IS". VIRIDITY ENERGY, INC.
 * HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */
package com.foo.scada

import scala.collection.SortedSet

/**
 * Calculator for calculating time series values.
 */
trait TimeSeriesCalculator {

  /**
   * Given a set of time series data, and an argument specifying how many minutes to average,
   * it will average the last X minutes of time series data. If there is less data than the
   * specified minutes to average for, the size of the data will be used as the divisor.
   * If there are any entries that have a quality that is Bad, those entries will be filtered out
   * prior to averaging.
   *
   * Note: Since this calculator will be hit many times, especially for the back-filling service,
   * the  time series data will be expected to be sorted and by epoc prior to being passed into this method.
   *
   *
   * @param minutes Number of minutes to average
   * @param timeSeries Series of time and value data
   */
  def calculate(minutes: Int, timeSeries: SortedSet[TimeSeriesEntry]): BigDecimal = {
    assert(minutes > 0, "Minutes for averaging must be greater than or equal to 1")

    val filteredSeries = timeSeries.filter(_.quality != Bad)

    if (filteredSeries.size > 0) {
      val divisor = filteredSeries.size match {
        case size if size < minutes ⇒ size
        case _                      ⇒ minutes
      }

      filteredSeries.slice(filteredSeries.size - minutes, filteredSeries.size).map { _.value }.sum / divisor
    }
    else 0.0
  }

}

case class TimeSeriesEntry(epoc: Long, value: BigDecimal, quality: ReadingQuality)
case class TimeAverage(minutes: Int, average: BigDecimal)