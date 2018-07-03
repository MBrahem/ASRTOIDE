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
package fr.uvsq.adam.astroide.optimizer

import adql.query._
import adql.query.constraint.Comparison
import adql.query.from.{ADQLJoin, InnerJoin}
import adql.search.SimpleReplaceHandler

class TranslatorJoin extends SimpleReplaceHandler(true, false) {


  override def `match`(obj: ADQLObject): Boolean = {
    try {
	val join = obj.asInstanceOf[InnerJoin].getJoinCondition.get(0).asInstanceOf[Comparison]

	return (join.getLeftOperand().getName.contains("SphericalDistance"))

    }

    catch {
      case cce: ClassCastException ⇒ false
    }
  }

  override def getReplacer(obj: ADQLObject): ADQLObject = {
    try {

      val join = obj.asInstanceOf[ADQLJoin]

      val leftTable = join.getLeftTable
      val rightTable = join.getRightTable

      return new InnerJoin(leftTable, rightTable, join.getJoinCondition)
    }

    catch {
      case cce: ClassCastException ⇒ return null
    }
  }
}

