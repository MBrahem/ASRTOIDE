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

import scala.io.Source

object Boundaries {

  def intersectWithBoundaries(x: List[Long], y: List[Long], i: Int): List[Long] = {

    if (x(i) > y(i + 1) || x(i + 1) < y(i)) {
      return Nil
    }
    return List(x(i - 1), y(i - 1), math.max(x(i), y(i)), math.min(x(i + 1), y(i + 1)))
  }

  def intersectWithElement(x: Long, bl: List[List[Long]]) = for {

    b <- bl
    lu = Range.Long(b(1), b(2) + 1, 1)
    if (lu.contains(x))
  } yield b(0)

  def convertToRange(boundaries: List[List[Long]]) = for {

    i <- boundaries
    ranges = (i(0), i(1), Range.Long(i(2), i(3) + 1, 1))

  } yield ranges

  def convertToRange2(boundaries: List[List[Long]]) = for {

    i <- boundaries
    ranges = (Range.Long(i(1), i(2) + 1, 1))

  } yield ranges

  @throws(classOf[java.io.IOException])
  def ReadFromFile(file: String): List[List[Long]] = {

    var boundaries = Source.fromFile(file).getLines.map { line =>
      line.drop(1).dropRight(1).split(",").map(_.toLong)
        .toList
    }.toList

    return boundaries

  }
}
