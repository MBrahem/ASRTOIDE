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

package fr.uvsq.adam.astroide.util

import org.apache.spark.broadcast.Broadcast

import scala.collection.Map

class Arguments {

  private var inputFile1: String = " "
  private var inputFile2: String = " "
  private var order: Int = 12
  private var queryFile: String = " "
  private var hdfs: String = " "
  private var radius: Double = 0.0027
  private var median: Double = 1
  private var pruningRadius: Double = 1
  private var id1: String = " "
  private var id2: String = " "
  private var limit: Int = 0
  private var histogram = scala.collection.Map(0L -> 0L)
  private var histogramS = scala.collection.Map(0L -> 0L)


  def getFile1(): String = {
    return this.inputFile1
  }

  def setFile1(inputFile1: String) {
    this.inputFile1 = inputFile1
  }

  def getFile2(): String = {
    return this.inputFile2
  }

  def setFile2(inputFile2: String) {
    this.inputFile2 = inputFile2
  }

  def getOrder(): Int = {
    return this.order
  }

  def setOrder(order: Int) {
    this.order = order
  }

  def getQueryFile(): String = {
    return this.queryFile
  }

  def setQueryFile(queryFile: String) {
    this.queryFile = queryFile
  }

  def getHDFS(): String = {
    return this.hdfs
  }

  def setHDFS(hdfs: String) {
    this.hdfs = hdfs
  }

    def getRadius(): Double = {
	return this.radius
    }

    def setRadius(radius: Double) {
	this.radius = radius
    }

    def getMedian(): Double = {
	return this.median
    }

    def setMedian(median: Double) {
	this.median = median
    }


    def getPruningRadius(): Double = {
	return this.pruningRadius
    }

    def setPruningRadius(radius: Double) {
	this.pruningRadius = pruningRadius
    }


  def getid1(): String = {
    return this.id1
  }

  def setid1(id1: String) {
    this.id1 = id1
  }

  def getid2(): String = {
    return this.id2
  }

  def setid2(id2: String) {
    this.id2 = id2
  }


  def getLimit(): Int = {
    return this.limit
  }

  def setLimit(limit: Int) {
    this.limit = limit
  }


  def getHistogram(): scala.collection.Map[Long, Long] = {
    return this.histogram
  }

  def setHistogram(histogram: scala.collection.Map[Long, Long]) {
    this.histogram = histogram
  }


  def getHistogramS(): scala.collection.Map[Long, Long] = {
    return this.histogramS
  }

  def setHistogramS(histogramS: scala.collection.Map[Long, Long]) {
    this.histogramS = histogramS
  }


}
