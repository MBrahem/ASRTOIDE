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
package fr.uvsq.adam.astroide.queries.basic

import fr.uvsq.adam.astroide.AstroideUDF.sphericalDistance
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

object KNNSQL {

  def Execute(df: DataFrame, ra: Double, dec: Double, k: Int): DataFrame = {

    val sphe_dist = udf((a: Double, d: Double) => sphericalDistance(a, d, ra, dec))

    val resultdf = df.withColumn("distance", sphe_dist(col("ra"), col("dec"))).orderBy(col("distance")).limit(k)

    return resultdf

  }
}


