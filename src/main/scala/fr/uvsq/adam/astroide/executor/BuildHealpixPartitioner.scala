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

import fr.uvsq.adam.astroide.partitioner.HealpixPartitioner
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.uvsq.adam.astroide.util.DirCheck.dirExists
import java.io.IOException

import fr.uvsq.adam.astroide.AstroideSession
import healpix.essentials.Pointing
import org.apache.commons.io.FilenameUtils

import Console.{BLUE, GREEN, RED,RESET}
import scala.io.Source

object BuildHealpixPartitioner extends AstroideSession {
  val usage =

    """
      |Usage: BuildHealpixPartitioner -fs hdfs://... schema(optional) infile separator outfile.parquet hdfscapacity healpixlevel column1 column2 boundariesfile
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {

      case Nil => map
      case "-fs" :: hdfs :: tail =>
        parseArguments(map ++ Map('hdfs -> hdfs), tail)
      case schema :: infile :: separator :: outfile :: capacity :: healpixlevel :: column1 :: column2 :: boundariesfile :: Nil =>
        map ++ Map('schema -> schema) ++ Map('infile -> infile) ++ Map('separator -> separator) ++ Map('outfile -> outfile) ++ Map('capacity -> capacity.toDouble) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('column1 -> column1) ++ Map('column2 -> column2) ++ Map('boundariesfile -> boundariesfile)
      case infile :: separator :: outfile :: capacity :: healpixlevel :: column1 :: column2 :: boundariesfile :: Nil =>
        map ++ Map('schema -> None) ++ Map('infile -> infile) ++ Map('separator -> separator) ++ Map('outfile -> outfile) ++ Map('capacity -> capacity.toDouble) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('column1 -> column1) ++ Map('column2 -> column2) ++ Map('boundariesfile -> boundariesfile)
      case option :: tail =>
        println(usage)
        throw new IllegalArgumentException(s"${RED}Unknown argument $option"+RESET);
    }

  }

  def main(args: Array[String]) {

    val configuration = parseArguments(Map(), args.toList)
    println(configuration)

    val schema = configuration('schema).toString
    val hdfs = configuration('hdfs).toString
    var input = configuration('infile).toString
    val separator = configuration('separator).toString
    val output = configuration('outfile).toString
    val capacity_hdfs = configuration('capacity).asInstanceOf[Double]
    val level_healpix = configuration('healpixlevel).asInstanceOf[Int]
    val column1 = configuration('column1).toString
    val column2 = configuration('column2).toString
    val boundaries = configuration('boundariesfile).toString

    import astroideSession.implicits._

      if (!dirExists(input, hdfs)) {
	    throw new IOException(s"${RED}Input file " + input + " does not exist"+RESET)
      }

      val format = List("csv", "gz")

      if (!format.contains(FilenameUtils.getExtension(input)))
	    throw new Exception(s"${RED}Input file " + input + " should be in csv format"+RESET)


      val Dataframe = if (schema == "None") {
      astroideSession.read.format("csv").option("delimiter", separator).option("header", true).load(input)
      }
      else {
      val schemaString = Source.fromFile(schema).getLines.mkString
      val structSchema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
      astroideSession.read.format("csv").option("delimiter", separator).option("header", true).schema(structSchema).load(input)
      }

      if (level_healpix < 0 || level_healpix > 29) {
      throw new Exception(s"${RED}HEALPix order should be in range [0,29]"+RESET)
      }


    try {

      val healpixClass =new Pointing(0, 0)

	    val start = System.currentTimeMillis()

      HealpixPartitioner.CreatePartitions(astroideSession, input, Dataframe, output, capacity_hdfs, level_healpix, column1, column2, boundaries)

	    val timeTotal = System.currentTimeMillis() - start

	    println(s"${GREEN}Total partitioning time " + timeTotal / 1000 + " sec "+RESET)

      astroideSession.stop()
    }

    catch {
      case a:java.lang.ClassNotFoundException => println(s"${RED}Please defind a valid class path to HEALPix library " + a.getMessage+RESET)
      case j: IOException => println(s"${RED}Error occurred while writing boundaries " + j.getMessage+RESET)
      case p: Exception ⇒ println(s"${RED}Error occurred while partitioning " + p.getMessage+RESET)
    }


  }
}
