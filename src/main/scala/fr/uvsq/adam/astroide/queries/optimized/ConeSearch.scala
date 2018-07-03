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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object ConeSearch {

  implicit class DataFrameCone(df: DataFrame) {

    def ExecuteConeSearch(healpixlevel: Int, column1: String, column2: String, ra: Double, dec: Double, radius: Double, boundariesFile: String): DataFrame = {

      var boundaries = Boundaries.ReadFromFile(boundariesFile)

      var coneSearchCells = ConeSearchCells.getCells(healpixlevel, ra, dec, radius).sorted

      var cone = ConeSearchCells.groupByRange(coneSearchCells)

      val intersection = for {
        b <- boundaries;
        c <- cone
        r = ConeSearchCells.intersectCone(b, c)
        if r != Nil
      } yield r

      val intersectiongroup = intersection.groupBy(w => w(0)).mapValues(l => l.map(x => Range.Long(x(1), x(2) + 1, 1)).flatten)

      val sphe_dist = udf((a: Double, d: Double) => sphericalDistance(a, d, ra, dec))

      val resultdf = df.where(col("nump").isin(intersectiongroup.keys.toSeq: _*)).where(col("ipix").isin(intersectiongroup.values.flatten.toSeq: _*)).filter(sphe_dist(col(column1), col(column2)) < radius)
      return resultdf

    }
  }

}


