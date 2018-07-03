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

object MainTranslator {

  def translateWithRules(query: ADQLObject): String = {

    val translatorJoin = new TranslatorJoin()
    val translatorContains = new TranslatorContains()
    val translatorDistance = new TranslatorDistance()
    val translator = new AstroideTranslator()

    translatorContains.searchAndReplace(query)
    translatorJoin.searchAndReplace(query)
    translatorDistance.searchAndReplace(query)

    val newquery = translator.translate(query)
    return newquery
  }

  def translateWithoutRules(query: ADQLObject): String = {
    val translator = new SparkTranslator()

    val newquery = translator.translate(query)
    return newquery
  }

}
