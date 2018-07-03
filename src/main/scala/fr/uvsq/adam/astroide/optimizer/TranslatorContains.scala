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

import adql.query.ADQLObject
import adql.search.SimpleReplaceHandler
import adql.query.constraint.Comparison
import adql.query.operand.function.geometry.PointFunction
import adql.query.operand.function.geometry.ContainsFunction
import adql.query.operand.function.geometry.CircleFunction
import adql.query.operand.function.DefaultUDF
import adql.query.constraint.ComparisonOperator

class TranslatorContains extends SimpleReplaceHandler(true, false) {


  override def `match`(obj: ADQLObject): Boolean = {
    try {
      val comp = obj.asInstanceOf[Comparison]

      return (comp.getLeftOperand().isInstanceOf[ContainsFunction] || comp.getRightOperand().isInstanceOf[ContainsFunction])

    }

    catch {
      case cce: ClassCastException ⇒ false
    }
  }

  override def getReplacer(obj: ADQLObject): ADQLObject = {
    try {

      val comp = obj.asInstanceOf[Comparison]

      val operand = if ((comp.getLeftOperand().isInstanceOf[ContainsFunction]) && (comp.getOperator() == ComparisonOperator.EQUAL) && comp.getRightOperand().isNumeric()) {
        comp.getLeftOperand()

      }
      else if ((comp.getRightOperand().isInstanceOf[ContainsFunction]) && (comp.getOperator() == ComparisonOperator.EQUAL) && comp.getLeftOperand().isNumeric()) {

        comp.getRightOperand()

      }
      val contains = operand.asInstanceOf[ContainsFunction]

      val point = contains.getLeftParam.getValue.asInstanceOf[PointFunction]
      val circle = contains.getRightParam.getValue.asInstanceOf[CircleFunction]

      val leftOp = new DefaultUDF("SphericalDistance", Array(point.getCoord1, point.getCoord2, circle.getCoord1, circle.getCoord2))

      return new Comparison(leftOp, ComparisonOperator.LESS_THAN, circle.getRadius)
    }

    catch {
      case cce: ClassCastException ⇒ return null
    }
  }
}
