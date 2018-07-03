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

object KNNCells {

  def intersectCell(x: Long, y: List[Long]): Boolean = {

    if (x > y(2) || x < y(1)) {

      return false
    }
    else return true
  }

  def overlap(b: List[List[Long]], x: Long): List[Long] = {
      b.foreach { elem => if (intersectCell(x, elem)) return List(elem(0), elem(1), elem(2)) }.asInstanceOf[List[Long]]
  }

  def getRange(b: List[List[Long]], x: Long): List[Long] = {
    b.foreach { elem => if (elem(0) == x) return List(elem(1), elem(2)) }.asInstanceOf[List[Long]]
  }

}
