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
package fr.uvsq.adam.astroide.queries.optimized

import fr.uvsq.adam.astroide.AstroideUDF.sphericalDistance
import healpix.essentials._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import fr.uvsq.adam.astroide.queries.optimized.ConeSearch._

object KNN {

  implicit class DataFrameKNN(df: DataFrame) {

    def ExecuteKNN(healpixlevel: Int, column1: String, column2: String, ra: Double, dec: Double, k: Int, boundariesFile: String): DataFrame = {

      var boundaries = Boundaries.ReadFromFile(boundariesFile)

      val theta = (90 - dec).toRadians
      val phi = ra.toRadians

      val ptg = new Pointing(theta, phi)

      val cell = HealpixProc.ang2pixNest(healpixlevel, ptg)
      var overlappingPartition = KNNCells.overlap(boundaries, cell)(0)

      val sphe_dist = udf((a: Double, d: Double) => sphericalDistance(a, d, ra, dec))

      val resultdf = df.where(col("nump") === overlappingPartition).withColumn("distance", sphe_dist(col("ra"), col("dec"))).orderBy(col("distance")).limit(k)

      val radius = resultdf.select(col("distance")).sort(col("distance"), col("distance").desc).first().getDouble(0)

      var coneSearchCells = ConeSearchCells.getCells(healpixlevel, ra, dec, radius).sorted

      val RangePartition = KNNCells.getRange(boundaries, overlappingPartition)

      if ((coneSearchCells.last <= RangePartition(1)) && (coneSearchCells.head >= RangePartition(0)))

        return resultdf

      else return df.ExecuteConeSearch(healpixlevel, column1, column2, ra, dec, radius, boundariesFile).withColumn("distance", sphe_dist(col(column1), col(column2))).orderBy(col("distance")).limit(k)

    }
  }

}


