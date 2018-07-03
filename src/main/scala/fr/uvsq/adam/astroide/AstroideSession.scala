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

package fr.uvsq.adam.astroide

import fr.uvsq.adam.astroide.optimizer.{AstroideRules, AstroideStrategies}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

trait AstroideSession {

  type ExtensionsBuilder = SparkSessionExtensions => Unit
  val f: ExtensionsBuilder = { e => e.injectOptimizerRule(AstroideRules.ConeSearchRule) }
  val j: ExtensionsBuilder = { e => e.injectOptimizerRule(AstroideRules.kNNRule) }
  val l: ExtensionsBuilder = { e => e.injectOptimizerRule(AstroideRules.kNNRuleProject) }
  val k: ExtensionsBuilder = { e => e.injectPlannerStrategy(AstroideStrategies.CrossMatchStrategy) }
    val p: ExtensionsBuilder = { e => e.injectOptimizerRule(AstroideRules.SphericalDistanceOptimizationRule) }
    val s: ExtensionsBuilder = { e => e.injectOptimizerRule(AstroideStrategies.MatchRule) }
    val v: ExtensionsBuilder = { e => e.injectResolutionRule(AstroideStrategies.knnJoinRule) }//
    val w: ExtensionsBuilder = { e => e.injectPlannerStrategy(AstroideStrategies.knnJoinStrategy) }
  val x: ExtensionsBuilder = { e => e.injectResolutionRule(AstroideStrategies.knnJoinRule2) }
  val m: ExtensionsBuilder = { e => e.injectOptimizerRule(AstroideStrategies.ProjectRule) }
  //val n: ExtensionsBuilder = { e => e.injectOptimizerRule(AstroideStrategies.ProjectRule2) }



  lazy val astroideSession: SparkSession = {
    SparkSession
      .builder()
      .appName("ASTROIDE")
	.withExtensions(s)
	.withExtensions(f)
	.withExtensions(j)
	.withExtensions(k)
	.withExtensions(l)
	.withExtensions(v)
	.withExtensions(w)
      .withExtensions(x)
      .withExtensions(m)
      //.withExtensions(n)
      .getOrCreate()
  }
}

