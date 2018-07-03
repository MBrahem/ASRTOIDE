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
package fr.uvsq.adam.astroide.executor

import scala.io.Source
import fr.uvsq.adam.astroide.queries.basic.KNNSQL
import org.apache.spark.sql.SparkSession
import fr.uvsq.adam.astroide.queries.optimized.Boundaries
import healpix.essentials._

object RunKNNSQL {

  val usage =

    """
      |Usage: RunKNNSQL infile alpha delta k
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {

      case Nil => map
      case infile :: ra :: dec :: k :: Nil =>
        map ++ Map('infile -> infile) ++ Map('ra -> ra.toDouble) ++ Map('dec -> dec.toDouble) ++ Map('k -> k.toInt)
      case option :: tail =>
        println(usage)
        throw new IllegalArgumentException(s"Unknown argument $option");
    }

  }

  def main(args: Array[String]) {

    val configuration = parseArguments(Map(), args.toList)
    println(configuration)

    val inputFile = configuration('infile).toString
    val alpha = configuration('alpha).asInstanceOf[Double]
    val delta = configuration('delta).asInstanceOf[Double]
    val k = configuration('k).asInstanceOf[Int]

    val spark = SparkSession.builder().appName("astroide").getOrCreate()
    import spark.implicits._

    val df = spark.read.parquet(inputFile)

    val resultdf = KNNSQL.Execute(df, alpha, delta, k)

    resultdf.show()

  }
}


