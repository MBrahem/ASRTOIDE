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

import fr.uvsq.adam.astroide.queries.optimized.ConeSearch._
import org.apache.spark.sql.SparkSession

object RunConeSearch {

  val usage =

    """
      |Usage: RunConeSearch infile.parquet healpixlevel column1 column2 ra dec radius boundariesfile
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {

      case Nil => map
      case infile :: healpixlevel :: column1 :: column2 :: ra :: dec :: radius :: boundariesfile :: Nil =>
        map ++ Map('infile -> infile) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('column1 -> column1) ++ Map('column2 -> column2) ++ Map('ra -> ra.toDouble) ++ Map('dec -> dec.toDouble) ++ Map('radius -> radius.toDouble) ++ Map('boundariesfile -> boundariesfile)
      case option :: tail =>
        println(usage)
        throw new IllegalArgumentException(s"Unknown argument $option");
    }

  }

  def main(args: Array[String]) {

    val configuration = parseArguments(Map(), args.toList)
    println(configuration)

    val inputFile = configuration('infile).toString
    val healpixlevel = configuration('healpixlevel).asInstanceOf[Int]
    val column1 = configuration('column1).toString
    val column2 = configuration('column2).toString
    val ra = configuration('ra).asInstanceOf[Double]
    val dec = configuration('dec).asInstanceOf[Double]
    val radius = configuration('radius).asInstanceOf[Double]
    val boundariesFile = configuration('boundariesfile).toString


    val spark = SparkSession.builder().appName("astroide").getOrCreate()
    import spark.implicits._

    val df = spark.read.parquet(inputFile)

    val resultdf = df.ExecuteConeSearch(healpixlevel, column1, column2, ra, dec, radius, boundariesFile)

    resultdf.show()


  }
}


