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

import healpix.essentials._
import fr.uvsq.adam.astroide.partitioner.PartitionerUtil
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, StringType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Encoders, Encoder}
import java.io._
import org.apache.spark.sql._

object JoinHash {

  val usage =

    """
      |Usage: HealpixPartitioner infile1.parquet infile2.parquet capacity healpixlevel
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {

      case Nil => map
      case "-fs" :: hdfs :: tail =>
        parseArguments(map ++ Map('hdfs -> hdfs), tail)
      case infile1 :: infile2 :: capacity :: healpixlevel :: Nil =>
        map ++ Map('infile1 -> infile1) ++ Map('infile2 -> infile2) ++ Map('capacity -> capacity.toDouble) ++ Map('healpixlevel -> healpixlevel.toInt)
      case option :: tail =>
        println(usage)
        throw new IllegalArgumentException(s"Unknown argument $option");
    }

  }

  def main(args: Array[String]) {

    val configuration = parseArguments(Map(), args.toList)
    println(configuration)

    val inputFile1 = configuration('infile1).toString
    val inputFile2 = configuration('infile2).toString
    val capacity = configuration('capacity).asInstanceOf[Double]
    val level = configuration('healpixlevel).asInstanceOf[Int]

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val DS1 = spark.read.parquet(inputFile1)
    val DS2 = spark.read.parquet(inputFile2)

    def udfToHealpix = udf((alpha: Double, delta: Double) => {

      val theta = math.Pi / 2 - delta.toRadians
      val phi = alpha.toRadians

      HealpixProc.ang2pixNest(level, new Pointing(theta, phi))
    })

    val DS1WithHealpix = DS1.withColumn("ipix", udfToHealpix($"alpha", $"delta"))
    val DS2WithHealpix = DS2.withColumn("ipix2", udfToHealpix($"alpha", $"delta"))

    val numpartition1 = PartitionerUtil.numPartition(DS1, capacity)
    val numpartition2 = PartitionerUtil.numPartition(DS2, capacity)

    spark.conf.set("spark.sql.shuffle.partitions", numpartition1)
    val DS1Sorted = DS1WithHealpix.repartition($"ipix").sortWithinPartitions($"ipix").cache()

    val DS2Sorted = DS2WithHealpix.repartition($"ipix2").sortWithinPartitions($"ipix2").cache()

    val joined = DS1Sorted.join(DS2Sorted, DS1Sorted("ipix") === DS2Sorted("ipix2"))

    joined.show()
    joined.explain

  }
}

