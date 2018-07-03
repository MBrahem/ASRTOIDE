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

import fr.uvsq.adam.astroide.AstroideUDF
import fr.uvsq.adam.astroide.executor.AstroideQueries.astroideVariables
import fr.uvsq.adam.astroide.queries.optimized.{Boundaries, ConeSearchCells, KNNCells}
import healpix.essentials.{HealpixProc, Pointing}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

object AstroideRules {


  case class SphericalDistanceOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {

      val newright = 0.05

      plan transformAllExpressions {

        case LessThan(left, right) if right.asInstanceOf[Literal].value.isInstanceOf[Double] && left.numberedTreeString.contains("SphericalDistance") =>

          LessThan(left, Literal(newright, DoubleType))
      }
    }
  }

  case class ConeSearchRule(spark: SparkSession) extends Rule[LogicalPlan] {

    @throws(classOf[Exception])
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {

      case f@Filter(condition, child)=>

        if (condition.toString().contains("SphericalDistance")) {
          val nump = child.outputSet.toList.find(x => x.toString().contains("nump"))
          val ipix = child.outputSet.toList.find(x => x.toString().contains("ipix"))

          var boundaries = Boundaries.ReadFromFile(astroideVariables.getFile2())

          val parameters = AstroideUDF.getParameters(condition.toString())

          var coneSearchCells = ConeSearchCells.getCells(astroideVariables.getOrder(), parameters(5).toDouble, parameters(6).toDouble, parameters(7).toDouble).sorted

          /*val newradius = parameters(7).toDouble / 2

          val newcondition = condition.transform{

            case LessThan(left, right)  =>

              LessThan(left, Literal(newradius, DoubleType))
          }*/

          val intervals = coneSearchCells.distinct.toSet[Any]

          val intervalsnump = ConeSearchCells.getIntervals(coneSearchCells, boundaries).keys.map(x=> Literal(x.toInt)).toSeq

          //val filter1 = In(ExpressionSet(nump).head, intervalsnump)

          //val filter2 = InSet(ExpressionSet(ipix).head, intervals)

          val newfilter = And(condition,And(In(ExpressionSet(nump).head, intervalsnump),InSet(ExpressionSet(ipix).head, intervals)))

          Filter(condition=newfilter,child)
        }
        else f
    }

  }


  case class kNNRule(spark: SparkSession) extends Rule[LogicalPlan] {

    @throws(classOf[Exception])
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {

      case l@LocalLimit(_, sort@Sort(sortOrder, true, p@Project(projectList, child))) =>

        val condition = p.expressions.mkString

        if (condition.contains("SphericalDistance")) {

          val parameters = AstroideUDF.getParameters(condition)

          var boundaries = Boundaries.ReadFromFile(astroideVariables.getFile2())

          val coordinates1 = parameters(5)
          val coordinates2 = parameters(6)

          val theta = (90 - (coordinates2.toDouble)).toRadians
          val phi = (coordinates1.toDouble).toRadians

          val ptg = new Pointing(theta, phi)

          val cell = HealpixProc.ang2pixNest(astroideVariables.getOrder(), ptg)
          var overlappingPartition = KNNCells.overlap(boundaries, cell)
          val nump = child.outputSet.toList.find(x => x.toString().contains("nump"))

          val newfilter = EqualTo(ExpressionSet(nump).head, Literal(overlappingPartition(0).toInt))

          var coneSearchCells = ConeSearchCells.getCells(astroideVariables.getOrder(), parameters(5).toDouble, parameters(6).toDouble, astroideVariables.getRadius()).sorted

          val RangePartition = KNNCells.getRange(boundaries, overlappingPartition(0))

          if ((coneSearchCells.last <= RangePartition(1)) && (coneSearchCells.head >= RangePartition(0))) {
            l.copy(child = Sort(sortOrder, true, p.copy(child = Filter(newfilter, child))))
          }


          else {

            val intervals = coneSearchCells.toSet[Any]

            val intervalsnump = ConeSearchCells.getIntervals(coneSearchCells, boundaries).keys.map(x => Literal(x.toInt)).toSeq

            val filter1 = In(ExpressionSet(nump).head, intervalsnump)

            val ipix = child.outputSet.toList.find(x => x.toString().contains("ipix"))

            val filter2 = InSet(ExpressionSet(ipix).head, intervals)

            val newfilter = And(filter1, filter2)

            l.copy(child = Sort(sortOrder, true, p.copy(child = Filter(newfilter, child))))
          }

        }
        else l

    }
  }

  case class kNNRuleProject(spark: SparkSession) extends Rule[LogicalPlan] {

    @throws(classOf[Exception])
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {

      case l@LocalLimit(_, Project(plist1, sort@Sort(sortOrder, true, p@Project(projectList, child)))) =>

        val condition = p.expressions.mkString

        if (condition.contains("SphericalDistance")) {

          val parameters = AstroideUDF.getParameters(condition)

          var boundaries = Boundaries.ReadFromFile(astroideVariables.getFile2())

          val coordinates1 = parameters(5)
          val coordinates2 = parameters(6)

          val theta = (90 - (coordinates2.toDouble)).toRadians
          val phi = (coordinates1.toDouble).toRadians

          val ptg = new Pointing(theta, phi)

          val cell = HealpixProc.ang2pixNest(astroideVariables.getOrder(), ptg)
          var overlappingPartition = KNNCells.overlap(boundaries, cell)

          val nump = child.outputSet.toList.find(x => x.toString().contains("nump"))

          val newfilter = EqualTo(ExpressionSet(nump).head, Literal(overlappingPartition(0).toInt))

          var coneSearchCells = ConeSearchCells.getCells(astroideVariables.getOrder(), parameters(5).toDouble, parameters(6).toDouble, astroideVariables.getRadius()).sorted

          val RangePartition = KNNCells.getRange(boundaries, overlappingPartition(0))

          if ((coneSearchCells.last <= RangePartition(1)) && (coneSearchCells.head >= RangePartition(0))) {
            l.copy(child = Project(plist1, Sort(sortOrder, true, p.copy(child = Filter(newfilter, child)))))
          }

          else {

            val intervals = coneSearchCells.toSet[Any]

            val intervalsnump = ConeSearchCells.getIntervals(coneSearchCells, boundaries).keys.map(x => Literal(x.toInt)).toSeq

            val filter1 = In(ExpressionSet(nump).head, intervalsnump)

            val ipix = child.outputSet.toList.find(x => x.toString().contains("ipix"))

            val filter2 = InSet(ExpressionSet(ipix).head, intervals)

            val newfilter = And(filter1, filter2)

            l.copy(child = Project(plist1, Sort(sortOrder, true, p.copy(child = Filter(newfilter, child)))))
          }



        }
        else l

    }
  }


}
