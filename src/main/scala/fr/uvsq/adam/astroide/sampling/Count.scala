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
package fr.uvsq.adam.astroide.sampling

import org.apache.spark.sql.SparkSession

object Count {

  val usage =

    """
      |Usage: Count infile
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {

      case Nil => map
      case "-fs" :: hdfs :: tail =>
        parseArguments(map ++ Map('hdfs -> hdfs), tail)
      case infile :: Nil =>
        map ++ Map('infile -> infile)
      case option :: tail =>
        println(usage)
        throw new IllegalArgumentException(s"Unknown argument $option");
    }

  }

  def main(args: Array[String]) {

    val configuration = parseArguments(Map(), args.toList)
    println(configuration)

    val inputFile = configuration('infile).toString
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val inputData = spark.read.parquet(inputFile)

    val elements = inputData.count()

    println("Number of elements" + elements + " File" + inputFile)

  }
}

