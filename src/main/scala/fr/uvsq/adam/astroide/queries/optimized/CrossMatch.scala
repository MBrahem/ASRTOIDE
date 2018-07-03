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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Encoders, Encoder}
import healpix.essentials._

object CrossMatch {

  implicit class DataFrameCross(inputData1: DataFrame) {

    def ExecuteXMatch(spark: SparkSession, inputData2: DataFrame, radius: Double, healpixlevel: Int): DataFrame = {

      import spark.implicits._

      def udfNeighbours = udf((ipix: Long) => {

        ipix +: HealpixProc.neighboursNest(healpixlevel, ipix)

      })

      val explodedData2 = inputData2.withColumn("ipix_neigh", explode(udfNeighbours($"ipix_2")))

      val joined = inputData1.join(explodedData2, explodedData2.col("ipix_neigh") === inputData1.col("ipix"))

      val sphe_dist = udf((a: Double, d: Double, a2: Double, d2: Double) => sphericalDistance(a, d, a2, d2))
      val result = joined.filter(sphe_dist($"ra", $"dec", $"ra_2", $"dec_2") < radius)

      implicit var encoder = RowEncoder(result.schema)

      implicit def tuple2[A1, A2](
                                   implicit e1: Encoder[A1],
                                   e2: Encoder[A2]): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1, e2)

      return result

    }
  }

}
