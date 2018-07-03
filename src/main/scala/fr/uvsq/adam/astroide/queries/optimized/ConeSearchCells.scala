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
package fr.uvsq.adam.astroide.queries.optimized

import healpix.essentials._

import scala.collection.immutable.Range

object ConeSearchCells {

  def getCells(order: Int, alpha: Double, delta: Double, radius: Double): List[Long] = {

    val f: Int = 4

    val theta = (90 - delta).toRadians
    val phi = alpha.toRadians

    val ptg = new Pointing(theta, phi)
    val list = HealpixProc.queryDiscInclusiveNest(order, ptg, radius.toRadians, f)

    return list.toArray().toList

  }

  def groupByRange(in: List[Long], acc: List[List[Long]] = Nil): List[List[Long]] = (in, acc) match {

    case (Nil, a) => a.map(_.reverse).reverse
    case (n :: tail, (last :: t) :: tailAcc) if n == last + 1 => groupByRange(tail, (n :: t) :: tailAcc)
    case (n :: tail, a) => groupByRange(tail, (n :: n :: Nil) :: a)
  }


  def intersectCone(x: List[Long], y: List[Long]): List[Long] = {

    if (x(1) > y(1) || x(2) < y(0)) {

      return Nil
    }
    return List(x(0), math.max(x(1), y(0)), math.min(x(2), y(1)))
  }

  def getIntervals(coneSearchCells: List[Long], boundaries: List[List[Long]]): Map[Long, List[Long]] = {

    var cone = ConeSearchCells.groupByRange(coneSearchCells)

    val intersection = for {
      b <- boundaries;
      c <- cone
      r = ConeSearchCells.intersectCone(b, c)
      if r != Nil
    } yield r

    val intersectiongroup = intersection.groupBy(w => w(0)).mapValues(l => l.map(x => Range.Long(x(1), x(2) + 1, 1)).flatten)
    return intersectiongroup

  }

}


