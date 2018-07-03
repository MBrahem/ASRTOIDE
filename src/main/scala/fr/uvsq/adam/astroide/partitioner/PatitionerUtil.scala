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

import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.DataFrame

object PartitionerUtil {

  def DFSize(df: DataFrame): Double = {

    //val dfsize = SizeEstimator.estimate(df)
    val dfsize = df.count()*(df.first().size*8)
    return dfsize / (1024 * 1024)

  }

  def numPartition(df: DataFrame, partitionSize: Double): Int = {

    val dfSize = PartitionerUtil.DFSize(df)
    val np = math.ceil(dfSize * 1.3 / partitionSize)

    return (np.toInt)

  }

  def parquetSize(file: String): Double = {

    val hdfscf: org.apache.hadoop.fs.FileSystem =
      org.apache.hadoop.fs.FileSystem.get(
        new org.apache.hadoop.conf.Configuration())

    val hadoopPath = new org.apache.hadoop.fs.Path(file)

    val recursive = false
    val ri = hdfscf.listFiles(hadoopPath, recursive)
    val it = new Iterator[org.apache.hadoop.fs.LocatedFileStatus]() {
      override def hasNext = ri.hasNext

      override def next() = ri.next()
    }
    val files = it.toList

    val size = files.map(_.getLen).sum
    val sizeMB = size / (1024 * 1024)
    return sizeMB
  }


}




