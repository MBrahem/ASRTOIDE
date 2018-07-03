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
import adql.translator.{PostgreSQLTranslator, TranslationException}

class AstroideTranslator extends PostgreSQLTranslator {


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
  override def translate(query: ADQLQuery): String = {

    val sql = StringBuilder.newBuilder

    sql.append((translate(query.getSelect())))

    sql.append("\nFROM ").append(translate(query.getFrom()))


    if (!query.getWhere().isEmpty())
      sql.append('\n').append(translate(query.getWhere()))


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