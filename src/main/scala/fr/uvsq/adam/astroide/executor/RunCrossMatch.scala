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

import fr.uvsq.adam.astroide.queries.optimized.CrossMatch._
import org.apache.spark.sql.SparkSession


object RunCrossMatch {

  val usage =

    """
      |Usage: RunCrossMatch infile infile2 radius healpixlevel
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {

      case Nil => map
      case infile :: infile2 :: radius :: healpixlevel :: Nil =>
        map ++ Map('infile -> infile) ++ Map('infile2 -> infile2) ++ Map('radius -> radius.toDouble) ++ Map('healpixlevel -> healpixlevel.toInt)
      case option :: tail =>
        println(usage)
        throw new IllegalArgumentException(s"Unknown argument $option");
    }

  }

  def main(args: Array[String]) {

    val configuration = parseArguments(Map(), args.toList)
    println(configuration)

    val inputFile_1 = configuration('infile).toString
    val inputFile_2 = configuration('infile2).toString
    val radius_cross = configuration('radius).asInstanceOf[Double]
    val healpixlevel = configuration('healpixlevel).asInstanceOf[Int]

    val spark = SparkSession.builder().appName("astroide").getOrCreate()
    import spark.implicits._

    val inputData_1 = spark.read.parquet(inputFile_1)

    var inputData_2 = spark.read.parquet(inputFile_2)

    val columnname = inputData_2.columns
    val newnames = columnname.map(x => x + "_2")

    for (i <- newnames.indices) {
      inputData_2 = inputData_2.withColumnRenamed(columnname(i), newnames(i))
    }

    val output = inputData_1.ExecuteXMatch(spark, inputData_2, radius_cross, healpixlevel)

    println(output.count())


      spark.stop()
  }

}

