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
import adql.query.operand.function.geometry.DistanceFunction
import adql.query.operand.function.geometry.PointFunction
import adql.query.operand.function.DefaultUDF

class TranslatorDistance extends SimpleReplaceHandler(true, false) {


  override def `match`(obj: ADQLObject): Boolean = {
    try {
      val distance = obj.asInstanceOf[DistanceFunction]

      val point1 = distance.getP1.getValue.asInstanceOf[PointFunction]
      val point2 = distance.getP2.getValue.asInstanceOf[PointFunction]

      val coordinates11 = point1.getCoord1
      val coordinates12 = point1.getCoord2

      val coordinates21 = point2.getCoord1
      val coordinates22 = point2.getCoord2

      if (coordinates11.isString() && coordinates21.isNumeric())
        return coordinates11.getName.equalsIgnoreCase("ra") && coordinates12.getName.equalsIgnoreCase("dec")

      else coordinates21.getName.equalsIgnoreCase("ra") && coordinates22.getName.equalsIgnoreCase("dec")

    }

    catch {
      case cce: ClassCastException ⇒ false
    }
  }

  override def getReplacer(obj: ADQLObject): ADQLObject = {
    try {

      val distance = obj.asInstanceOf[DistanceFunction]

      val point1 = distance.getP1.getValue.asInstanceOf[PointFunction]
      val point2 = distance.getP2.getValue.asInstanceOf[PointFunction]

      val coordinates11 = point1.getCoord1
      val coordinates12 = point1.getCoord2

      val coordinates21 = point2.getCoord1
      val coordinates22 = point2.getCoord2

      return new DefaultUDF("SphericalDistance", Array(coordinates11, coordinates12, coordinates21, coordinates22))

    }

    catch {
      case cce: ClassCastException ⇒ return null
    }
  }
}
    




