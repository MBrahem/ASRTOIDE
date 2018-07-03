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
import fr.uvsq.adam.astroide.queries.optimized.KNNJoinCells
import healpix.essentials.{HealpixProc, Pointing}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.{SparkSession, Strategy, execution}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.execution.window.WindowExec

object AstroideStrategies {

    def duplicate(ipix: Long): Array[Long] = {
	return HealpixProc.neighboursNest(astroideVariables.getOrder(), ipix).filter(_ >= 0)

    }

    def duplicatePlus(array: Array[Long]): Array[Long] = {
	return array.flatMap(x => x +: HealpixProc.neighboursNest(astroideVariables.getOrder(), x)).filter(_ >= 0).distinct
    }

    def repeteDuplication(n: Int, r: Array[Long]): Array[Long] = {
	return (1 to n).foldLeft(r)((rx, _) => duplicatePlus(rx))
    }

    case class MatchRule(spark: SparkSession) extends Rule[LogicalPlan] {

	@throws(classOf[Exception])
	override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {

	    case Project(projectList, r@LogicalRelation(t, relationList, _)) =>

		val ipix = relationList.find(x => x.toString().contains("ipix"))

		Project(projectList ++ ipix, r)
	}

    }


	case class ProjectRule(spark: SparkSession) extends Rule[LogicalPlan] {

		@throws(classOf[Exception])
		override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {

			case Project(projectList,r@LogicalRelation(t, relationList, _)) =>

				val ipix = relationList.find(x => x.toString().contains("ipix10"))

				Project(projectList++ipix, r)
		}

	}


	case class ProjectRule2(spark: SparkSession) extends Rule[LogicalPlan] {

		@throws(classOf[Exception])
		override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {

			case Project(projectList,Filter(xx, r@LogicalRelation(t, relationList, _))) =>

				val ipix = relationList.find(x => x.toString().contains("neighbours"))

				Project(projectList++ipix, Filter(xx,r))
		}

	}


    case class knnJoinRule(spark: SparkSession) extends Rule[LogicalPlan] {

			@throws(classOf[Exception])
			override def apply(plan: LogicalPlan): LogicalPlan = plan transform {

				case g@GlobalLimit(_, l@LocalLimit(limit, p@Project(plist, Sort(sortOrder, true, Project(_, SubqueryAlias(_, Project(_, SubqueryAlias(_, SubqueryAlias(_, child))))))))) => {

					LocalLimit(limit, Project(plist, child))

				}

			}
		}


			case class knnJoinRule2(spark: SparkSession) extends Rule[LogicalPlan] {

				@throws(classOf[Exception])
				override def apply(plan: LogicalPlan): LogicalPlan = plan transform {



					case f@Filter(filterCondition,j@Join(left,right, Cross, _)) if filterCondition.toString().contains("IN") && (filterCondition.references.contains(right.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals(astroideVariables.getid2())).last.toAttribute))=> {

						val ra = left.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("ra")).last
						val dec = left.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("dec")).last

						val ra_2 = right.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("ra")).last
						val dec_2 = right.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("dec")).last

						val id1 = left.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals(astroideVariables.getid1())).last

						val id2 = right.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals(astroideVariables.getid2())).last


						val udfTest = ScalaUDF((v1: String, v2: String, v3: String, v4: String) => AstroideUDF.sphericalDistance(v1.toDouble, v2.toDouble, v3.toDouble, v4.toDouble), DoubleType, Seq(ra, dec, ra_2, dec_2))

						val windowExpr = WindowExpression('rank.function('dist), WindowSpecDefinition(Seq(id1), Seq('dist.asc), UnspecifiedFrame)).as("rank")


						val newlist = Seq(id1, id2).union(Seq(udfTest.as("dist")))

						val cd = 'rank <= astroideVariables.getLimit()

						Filter(cd, Window(Seq(windowExpr), Seq(id1), Seq('dist.asc), Project(newlist,j)))
					}
				}
			}




	/*@throws(classOf[Exception])
	override def apply(plan: LogicalPlan): LogicalPlan = plan transform {

	    case Project(list, j@Join(SubqueryAlias(alias1, left), SubqueryAlias(alias2, g@GlobalLimit(_, l@LocalLimit(limit, sort@Sort(sortOrder, true, p@Project(projectList, child))))), Cross, _)) => {

		/*val fields = p.expressions.mkString

		val list = left.outputSet

		val ra = list.find(x=> x.toString().substring(0,x.toString().indexOf("#")).contentEquals("ra"))
		val dec = list.find(x=> x.toString().substring(0,x.toString().indexOf("#")).contentEquals("dec"))

		val udf = fields.substring(fields.toString().indexOf("SphericalDistance"), fields.toString().indexOf("AS")).replace("'R.ra",ra.last.toString())
		  .replace("'R.dec",dec.last.toString())

		val condition = LessThan(udf,Literal(0.0027))*/

		//val ipix1 = left.outputSet.find(x => x.toString().contains("ipix"))
		//val ipix = child.outputSet.find(x => x.toString().contains("ipix"))

		//val condition = And(EqualTo(Literal(1),Literal(1)), EqualTo(ipix1.last.expr,ipix.last.expr))

		val ra = left.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("ra")).last.toAttribute
		val dec = left.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("dec")).last.toAttribute

		val ra_2 = child.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("ra")).last.toAttribute
		val dec_2 = child.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("dec")).last.toAttribute

		val TYC1 = left.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("TYC1")).last.toAttribute

		val udfTest = ScalaUDF((v1: String, v2: String, v3: String, v4: String) => AstroideUDF.sphericalDistance(v1.toDouble, v2.toDouble, v3.toDouble, v4.toDouble), DoubleType, Seq(ra, dec, ra_2, dec_2))

		val pruningRadius = (0.00027777777) * math.ceil(limit.toString().toDouble / astroideVariables.getMedian())

		val condition = LessThan(udfTest, Literal(pruningRadius))
		astroideVariables.setPruningRadius(pruningRadius)

		val newlist = list.union(Seq(udfTest.as("dist")))

		val planProject = Project(newlist, j.copy(left = left, right = child, Cross, Option(condition)))

		val windowExpr = WindowExpression('rank.function('dist), WindowSpecDefinition(Seq(TYC1), Seq('dist.asc), UnspecifiedFrame)).as("rank")

		val cd = 'rank <= limit

		Project(list, Filter(cd, Window(Seq(windowExpr), Seq(TYC1), Seq('dist.asc), planProject)))

	    }

	}*/



	case class knnJoinStrategy(spark: SparkSession) extends Strategy {

@throws(classOf[Exception])
override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

  //case Project(list,j@Join(Project(_,left), Project(rlist,Join(t1,Project(_,l@LocalLimit(limit,t2)),LeftSemi,_)), Cross, _)) => {

	case j@Join(Project(l,left),Project(r,right), Cross, condition) => {



		val ipix1 = left.outputSet.find(x => x.toString().contains("neighbours"))

		val ra = right.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("ra")).last.toAttribute
		val dec = right.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("dec")).last.toAttribute


		val udfTest = ScalaUDF((alpha: String, delta: String) => HealpixProc.ang2pixNest(10, new Pointing(math.Pi / 2 - delta.toDouble.toRadians, alpha.toDouble.toRadians)), LongType, Seq(ra, dec))

		val newlist = r.union(Seq(udfTest.as("ipix10")))

		val right1 = Project(newlist, right)

		val ipix2 = right1.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("ipix10")).last.toAttribute

		val l1 = l ++ ipix1


		//val udfNeighbours = ScalaUDF((ipix: Long) => (KNNJoinCells.addNeighbours(ipix,astroideVariables.getHistogram(), astroideVariables.getHistogramS(), astroideVariables.getLimit(), astroideVariables.getOrder())+ipix).toList, ArrayType(LongType), Seq(ipix1))

		//astroideVariables.getHistogram().take(10).foreach(println)

		//println(astroideVariables.getLimit())
		//println(astroideVariables.getOrder())
			//addNeighbours(ipix,astroideVariables.getHistogram(), astroideVariables.getHistogramS(), astroideVariables.getLimit(), astroideVariables.getOrder() ), ArrayType(LongType), Seq(ipix1.last.toAttribute))


		//val explode = Explode(udfNeighbours)

		//val generated = left.generate(generator = explode, true, false, alias = Some("alias"), Seq.empty).analyze

		//val ipix3 = generated.output.last

		/*val ra = left.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("ra")).last.toAttribute
		val dec = left.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("dec")).last.toAttribute

		val ra_2 = t1.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("ra")).last.toAttribute
		val dec_2 = t1.outputSet.find(x => x.toString().substring(0, x.toString().indexOf("#")).contentEquals("dec")).last.toAttribute*/


		//val udfTest = ScalaUDF((v1: String, v2: String, v3: String, v4: String) => AstroideUDF.sphericalDistance(v1.toDouble, v2.toDouble, v3.toDouble, v4.toDouble), DoubleType, Seq(ra, dec, ra_2, dec_2))

		val left1 = Project(l1,left)

		//val right1 = Project(r1,right)
		//val newlist = list.union(Seq(udfTest.as("dist")))
		//CartesianProductExec( planLater(left1), planLater(right1), condition):: Nil

		execution.joins.SortMergeJoinExec(Seq(ipix1.last.expr), Seq(ipix2.expr), Inner, condition, planLater(left1), planLater(right1)) :: Nil

		//planProject :: Nil

		/*val windowExpr = WindowExpression('rank.function('dist), WindowSpecDefinition(Seq('source_id), Seq('dist.asc), UnspecifiedFrame)).as("rank")

		val cd = 'rank <= limit

		ProjectExec(list, FilterExec(cd, WindowExec(Seq(windowExpr), Seq('source_id), Seq('dist.asc), planProject))) :: Nil*/

		//execution.joins.SortMergeJoinExec(Seq(ipix1.last.expr), Seq(ipix3.expr), Inner, Option(condition).reduceOption(And), planLater(left), planLater(generated.analyze)) :: Nil

  }

  case _ => Nil

}
}


    /*case class knnJoinStrategy(spark: SparkSession) extends Strategy {

	@throws(classOf[Exception])
	override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

	    case j@Join(left, right, Cross, Some(condition)) if condition.toString().contains("ra") && condition.toString().contains("dec") => {

		val ipix1 = left.outputSet.find(x => x.toString().contains("ipix"))
		val ipix2 = right.outputSet.find(x => x.toString().contains("ipix"))

		val r = condition.toString().substring(condition.toString().indexOf("<")).split("""[(|,<|)]""")

		val n = (r(1).toDouble / 0.00027777777).toInt

		println("number of duplication: " + n)

		val udfNeighbours = ScalaUDF((ipix: Long) => ipix +: repeteDuplication(n, duplicate(ipix)), ArrayType(LongType), Seq(ipix2.last.toAttribute))


		val explode = Explode(udfNeighbours)

		val generated = right.generate(generator = explode, true, false, alias = Some("alias"), Seq.empty).analyze

		val ipix3 = generated.output.last

		execution.joins.SortMergeJoinExec(Seq(ipix1.last.expr), Seq(ipix3.expr), Inner, Option(condition).reduceOption(And), planLater(left), planLater(generated.analyze)) :: Nil

	    }

	    case _ => Nil

	}
    }*/


    case class CrossMatchStrategy(spark: SparkSession) extends Strategy {

    @throws(classOf[Exception])
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      case j@Join(left, right, _, Some(condition)) if condition.toString().contains("SphericalDistance") => {

        val ipix1 = left.outputSet.find(x => x.toString().contains("ipix"))
        val ipix = right.outputSet.find(x => x.toString().contains("ipix"))

        val udfNeighbours = ScalaUDF((ipix: Long) => ipix +: HealpixProc.neighboursNest(astroideVariables.getOrder(), ipix), ArrayType(LongType), Seq(ipix.last.toAttribute))

        val explode = Explode(udfNeighbours)

        val generated = right.generate(generator = explode, true, false, alias = Some("alias"), Seq.empty).analyze

        val ipix2 = generated.output.last

        execution.joins.SortMergeJoinExec(Seq(ipix1.last.expr), Seq(ipix2.expr), Inner, Option(condition).reduceOption(And), planLater(left), planLater(generated.analyze)) :: Nil

      }

      case _ => Nil

    }
  }

}


