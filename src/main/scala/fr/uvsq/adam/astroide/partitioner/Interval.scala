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

object Interval {

  def is_overlapping(x1: Long, x2: Long, y1: Long, y2: Long) = math.max(x1, y1) <= math.min(x2, y2)

  def intersectionList(l1: List[Long], l2: List[Long]) = if (is_overlapping(l1(1), l1(2), l2(0), l2(1))) List(l1(0), math.max(l1(1), l2(0)), math.min(l1(2), l2(1))) else Nil


}
