/**
  * *****************************************************************************
  * copyright 2018 ASTROIDE
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
  * use this file except in compliance with the License.  You may obtain a copy
  * of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
  * License for the specific language governing permissions and limitations under
  * the License.
  * ****************************************************************************
  */

package fr.uvsq.adam.astroide

import healpix.essentials._
import org.apache.spark.sql.SparkSession

object AstroideUDF {

  def sphericalDistance(RA1: Double, DEC1: Double, RA2: Double, DEC2: Double): Double = {

    val RA1_rad = RA1.toRadians
    val RA2_rad = RA2.toRadians
    val DEC1_rad = DEC1.toRadians
    val DEC2_rad = DEC2.toRadians
    var d = scala.math.pow(math.sin((DEC1_rad - DEC2_rad) / 2), 2)
    d += scala.math.pow(math.sin((RA1_rad - RA2_rad) / 2), 2) * math.cos(DEC1_rad) * math.cos(DEC2_rad)

    return (2 * math.asin(math.sqrt(d)).toDegrees)

  }

  def RegisterUDF(spark: SparkSession, order: Int) = {
    spark.udf.register("SphericalDistance", (x: Double, y: Double, x2: Double, y2: Double) => sphericalDistance(x, y, x2, y2))
    spark.udf.register("Neighbours", (ipix: Long) => ipix +: HealpixProc.neighboursNest(order, ipix))
  }

  def RegisterUDFWithRules(spark: SparkSession) = {
    spark.udf.register("SphericalDistance", (x: Double, y: Double, x2: Double, y2: Double) => sphericalDistance(x, y, x2, y2))
  }

  def getParameters(condition: String) = {

    condition.substring(condition.toString().indexOf("SphericalDistance")).split("""[(|,<|)]""").filterNot(s => s.isEmpty || s.trim.isEmpty)
  }


}
