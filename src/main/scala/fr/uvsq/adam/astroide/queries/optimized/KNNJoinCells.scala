package fr.uvsq.adam.astroide.queries.optimized

import healpix.essentials.HealpixProc
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{desc, lit}

import scala.collection.Map

object KNNJoinCells {

  def Neighbours(set: Set[Long], healpixlevel: Int): Set[Long] = {
    return set.flatMap(x => HealpixProc.neighboursNest(12, x)).filter(_ != -1)
  }


  def addNeighbours(a: Long, histogram: Broadcast[Map[Long, Long]], histogramS: Broadcast[Map[Long, Long]], k: Int, healpixlevel: Int): Set[Long] = {

    var ipix_precedant = Set(a)
    var limit = 3
    var Nb_objets = histogram.value.filterKeys(ipix_precedant).foldLeft(0L)(_ + _._2)
    var neighbours = Set.empty[Long]
    var neighboursGlobal = Set.empty[Long]
    var nb_voisins: Long = 0

    while ((Nb_objets < k) && (limit >= 0)) {
      neighbours = Neighbours(ipix_precedant, healpixlevel).diff(neighboursGlobal)

      nb_voisins = histogram.value.filterKeys(neighbours).foldLeft(0L)(_ + _._2)

      Nb_objets = Nb_objets + nb_voisins

      ipix_precedant = neighbours
      neighboursGlobal = neighboursGlobal ++ neighbours
      limit = limit - 1

    }

    val finalSet = neighboursGlobal.filter(histogramS.value.keySet)

    return finalSet


  }

  def CreateHistogramDF(inputData_2:DataFrame, id2:String): DataFrame={

    val hist = inputData_2.select(id2,"ipix10").groupBy("ipix10")
    val histCount = hist.count()
    var histS = histCount.sort(desc("count"))

    return histS


  }

  def CreateHistogramS(histS:DataFrame): Map[Long,Long]={

    /*val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")*/
    val histSMap = histS.rdd.map(row => (row(0).asInstanceOf[Long], row(1).asInstanceOf[Long])).collectAsMap()

    return histSMap

    //println("HistogramS data has been created and broadcasted")

  }



  def CreateHistogramAll(inputData_1:DataFrame, inputData_2:DataFrame, histS:DataFrame): Map[Long,Long]={

    /*val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")*/

    val histR = inputData_1.select("ipix").except(inputData_2.select("ipix")).withColumn("count",lit(0L))

    val histogram = histR.union(histS)


    val histogramMap = histogram.rdd.map(row => (row(0).asInstanceOf[Long], row(1).asInstanceOf[Long])).collectAsMap()

    //val br = sc.broadcast(histogramMap)

    //val brS = sc.broadcast(histSMap)

    return histogramMap

  }

}
