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

import adql.query.{ADQLQuery, SelectAllColumns, SelectItem}
import adql.query.constraint.{Comparison, ComparisonOperator}
import adql.query.from.ADQLJoin
import adql.query.operand.function.DefaultUDF
import adql.query.operand.function.geometry.{CircleFunction, ContainsFunction, DistanceFunction, PointFunction}
import adql.translator.{PostgreSQLTranslator, TranslationException}
import fr.uvsq.adam.astroide.executor.AstroSparkQueries.variables
import fr.uvsq.adam.astroide.queries.optimized.{Boundaries, ConeSearchCells, KNNCells}
import healpix.essentials._

class SparkTranslator extends PostgreSQLTranslator {

  @throws(classOf[TranslationException])
  override def translate(fct: DistanceFunction): String = {

    val str = StringBuilder.newBuilder

    val point1 = fct.getP1.getValue.asInstanceOf[PointFunction]
    val point2 = fct.getP2.getValue.asInstanceOf[PointFunction]

    val coordinates11 = point1.getCoord1
    val coordinates12 = point1.getCoord2

    val coordinates21 = point2.getCoord1
    val coordinates22 = point2.getCoord2

    val udf = new DefaultUDF("SphericalDistance", Array(coordinates11, coordinates12, coordinates21, coordinates22))

    str.append(udf.toADQL)

    return str.toString()
  }


  @throws(classOf[TranslationException])
  override def translate(join: ADQLJoin): String = {

    val sql = StringBuilder.newBuilder

    val comp = join.getJoinCondition.get(0).asInstanceOf[Comparison]

    val contains = if (comp.getLeftOperand().isInstanceOf[ContainsFunction] && (comp.getOperator() == ComparisonOperator.EQUAL) && comp.getRightOperand().isNumeric()) {
      comp.getLeftOperand()
    }
    else if (comp.getRightOperand().isInstanceOf[ContainsFunction] && (comp.getOperator() == ComparisonOperator.EQUAL) && comp.getLeftOperand().isNumeric()) {
      comp.getRightOperand()
    }

    val point = contains.asInstanceOf[ContainsFunction].getLeftParam.getValue.asInstanceOf[PointFunction]
    val circle = contains.asInstanceOf[ContainsFunction].getRightParam.getValue.asInstanceOf[CircleFunction]

    sql.append(translate(join.getLeftTable)).append(" JOIN ")

    val alias1 = join.getLeftTable.getName
    val alias2 = join.getRightTable.getName

    sql.append("\n(SELECT *, EXPLODE(Neighbours(" + alias2 + ".ipix)) AS ipix_nei FROM ").append(translate(join.getRightTable)).append(")").append(" AS " + join.getRightTable.getName)
    sql.append(" ON (" + alias1 + ".ipix=" + alias2 + ".ipix_nei)")
    sql.append("\nWHERE ").append(" (SphericalDistance(" + point.getParameter(1) + "," + point.getParameter(2) + "," + circle.getCoord1 + "," + circle.getCoord2 + ")" + " <" + circle.getRadius.getName + ")").toString()

  }


  @throws(classOf[TranslationException])
  def appendIdentifier(str: StringBuilder, id: String, caseSensitive: Boolean): StringBuilder = {

    return str.append(id)
  }

  override def translate(item: SelectItem): String = {

    if (item.isInstanceOf[SelectAllColumns])
      return "*"

    if (item.hasAlias()) {
      var str = StringBuilder.newBuilder.append(translate(item.getOperand()))
      str.append(" AS ")

      if (item.isCaseSensitive())
        appendIdentifier(str, item.getAlias(), true)
      else
        appendIdentifier(str, item.getAlias().toLowerCase(), true)

      return str.toString()
    }

    else {

      var str = StringBuilder.newBuilder.append(translate(item.getOperand()))

      return str.toString()

    }

  }

  @throws(classOf[TranslationException])
  override def translate(comp: Comparison): String = {

    if ((comp.getLeftOperand().isInstanceOf[ContainsFunction]) && (comp.getOperator() == ComparisonOperator.EQUAL) && comp.getRightOperand().isNumeric()) {
      val leftOperand = comp.getLeftOperand().asInstanceOf[ContainsFunction]

      val point = leftOperand.getLeftParam.getValue.asInstanceOf[PointFunction]
      val circle = leftOperand.getRightParam.getValue.asInstanceOf[CircleFunction]

      val leftOp = new DefaultUDF("SphericalDistance", Array(point.getCoord1, point.getCoord2, circle.getCoord1, circle.getCoord2))

      var str = StringBuilder.newBuilder.append(new Comparison(leftOp, ComparisonOperator.LESS_THAN, circle.getRadius).toADQL())

      var boundaries = Boundaries.ReadFromFile(variables.getFile2())

      var coneSearchCells = ConeSearchCells.getCells(variables.getOrder(), circle.getCoord1.getName.toDouble, circle.getCoord2.getName.toDouble, circle.getRadius.getName.toDouble).sorted

      var cone = ConeSearchCells.groupByRange(coneSearchCells)

      val intersection = for {
        b <- boundaries;
        c <- cone
        r = ConeSearchCells.intersectCone(b, c)
        if r != Nil
      } yield r

      val intersectiongroup = intersection.groupBy(w => w(0)).mapValues(l => l.map(x => Range.Long(x(1), x(2) + 1, 1)).flatten)


      val intervals = coneSearchCells.mkString(",")
      val intervalsnump = intersectiongroup.keys.toSeq.mkString(",")

      str.append(" AND nump IN (" + intervalsnump + ") AND ipix IN (" + intervals + ")")

      return str.toString()

    }

    if ((comp.getRightOperand().isInstanceOf[ContainsFunction]) && (comp.getOperator() == ComparisonOperator.EQUAL) && comp.getLeftOperand().isNumeric()) {
      val rightOperand = comp.getRightOperand().asInstanceOf[ContainsFunction]

      val point = rightOperand.getLeftParam.getValue.asInstanceOf[PointFunction]
      val circle = rightOperand.getRightParam.getValue.asInstanceOf[CircleFunction]

      val leftOp = new DefaultUDF("SphericalDistance", Array(point.getCoord1, point.getCoord2, circle.getCoord1, circle.getCoord2))

      var str = StringBuilder.newBuilder.append(new Comparison(leftOp, ComparisonOperator.LESS_THAN, circle.getRadius).toADQL())

      var boundaries = Boundaries.ReadFromFile(variables.getFile2())

      var coneSearchCells = ConeSearchCells.getCells(variables.getOrder(), circle.getCoord1.getName.toDouble, circle.getCoord2.getName.toDouble, circle.getRadius.getName.toDouble).sorted

      var cone = ConeSearchCells.groupByRange(coneSearchCells)

      val intersection = for {
        b <- boundaries;
        c <- cone
        r = ConeSearchCells.intersectCone(b, c)
        if r != Nil
      } yield r

      val intersectiongroup = intersection.groupBy(w => w(0)).mapValues(l => l.map(x => Range.Long(x(1), x(2) + 1, 1)).flatten)

      val intervals = intersectiongroup.values.flatten.toSeq.mkString(",")

      val intervalsnump = intersectiongroup.keys.toSeq.mkString(",")
      str.append(" AND nump IN (" + intervalsnump + ") AND ipix IN (" + intervals + ")")

      return str.toString()

    }

    else
      return super.translate(comp)
  }

  @throws(classOf[TranslationException])
  override def translate(query: ADQLQuery): String = {

    val sql = StringBuilder.newBuilder

    sql.append((translate(query.getSelect())))

    sql.append("\nFROM ").append(translate(query.getFrom()))


    if (!query.getWhere().isEmpty())
      sql.append('\n').append(translate(query.getWhere()))


    if (!query.getOrderBy().isEmpty() && query.getSelect.searchByAlias(query.getOrderBy.get(0).getColumnName).isInstanceOf[DistanceFunction] && query.getSelect().hasLimit()) {

      val alias = query.getOrderBy.get(0).getColumnName
      val distanceItem = query.getSelect.searchByAlias(alias)
      val dist = distanceItem.asInstanceOf[DistanceFunction]

      var boundaries = Boundaries.ReadFromFile(variables.getFile2())

      val point1 = dist.getP1.getValue.asInstanceOf[PointFunction]
      val point2 = dist.getP2.getValue.asInstanceOf[PointFunction]

      val coordinates1 = point2.getParameter(1).getName
      val coordinates2 = point2.getParameter(2).getName

      val theta = (90 - (coordinates2.toDouble)).toRadians
      val phi = (coordinates1.toDouble).toRadians

      val ptg = new Pointing(theta, phi)

      val cell = HealpixProc.ang2pixNest(variables.getOrder(), ptg)
      var overlappingPartition = KNNCells.overlap(boundaries, cell)(0)

      if (query.getWhere().isEmpty()) {
        sql.append("\nWHERE ").append("nump = " + overlappingPartition)
      }

      else {

        sql.append(" AND ").append("nump = " + overlappingPartition)
      }
    }

    if (!query.getGroupBy().isEmpty())
      sql.append('\n').append(translate(query.getGroupBy()))

    if (!query.getHaving().isEmpty())
      sql.append('\n').append(translate(query.getHaving()))

    if (!query.getOrderBy().isEmpty())
      sql.append('\n').append(translate(query.getOrderBy()))

    if (query.getSelect().hasLimit())
      sql.append("\nLimit ").append(query.getSelect().getLimit())

    return sql.toString()
  }


}