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
package fr.uvsq.adam.astroide.partitioner

import healpix.essentials._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.io._

import scala.Console.{BLUE, GREEN, MAGENTA, RED, RESET}

object HealpixPartitioner {

  def saveBoundaries(f: java.io.File)(op: java.io.PrintWriter => Unit) {

    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  @throws(classOf[Exception])
  def CreatePartitions(spark: SparkSession, input: String, inputData: DataFrame, outputFile: String, capacity: Double, level: Int, coordinates1: String, coordinates2: String, boundariesFile: String) {

    def udfToHealpix = udf((alpha: Double, delta: Double) => {

      val theta = math.Pi / 2 - delta.toRadians
      val phi = alpha.toRadians

      HealpixProc.ang2pixNest(level, new Pointing(theta, phi))
    })

    val inputDataWithHealpix = inputData.withColumn("ipix", udfToHealpix(col(coordinates1), col(coordinates2)))

    val numpartition = PartitionerUtil.numPartition(inputData, capacity)

    println(s"${BLUE}Number of partitions *****" + numpartition + "******"+RESET)

    spark.conf.set("spark.sql.shuffle.partitions", numpartition)
    val outputData = inputDataWithHealpix.sort(col("ipix"))


    /*implicit var encoder = RowEncoder(outputData.schema)

    implicit def tuple2[A1, A2](
                                 implicit e1: Encoder[A1],
                                 e2: Encoder[A2]): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1, e2)

    val newstructure = StructType(Seq(StructField("nump", IntegerType, true)).++(inputDataWithHealpix.schema.fields))

    val mapped = outputData.rdd.mapPartitionsWithIndex((id, iter) => {
      val data = iter.toList
      data.map(x => Row.fromSeq(id +: x.toSeq)).iterator
    }, false)

    val indexDF = spark.createDataFrame(mapped, newstructure)*/

    val indexDF = outputData.withColumn("nump", spark_partition_id())

    indexDF.write.mode(SaveMode.Overwrite).partitionBy("nump").bucketBy(10, "ipix").options(Map("path" -> outputFile)).saveAsTable("t")
    println(s"${BLUE}Partitioned output file " + outputFile + " is created on HDFS"+RESET)

    println(s"${MAGENTA}***** Please wait for metadata creation on "+boundariesFile+ "*****"+RESET)

    val boundaries = indexDF.groupBy(col("nump")).agg(first(col("ipix")), last(col("ipix"))).collect().toList

    saveBoundaries(new File(boundariesFile)) { p =>
      boundaries.foreach(p.println)
    }

    println(s"${BLUE}Boundaries file " + boundariesFile + " is saved "+RESET)


  }
}
