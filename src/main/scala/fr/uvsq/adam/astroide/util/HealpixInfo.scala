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
package fr.uvsq.adam.astroide.util

import healpix.essentials.HealpixBase
import healpix.essentials.Scheme
import healpix.essentials.Pointing

object HealpixInfo {

  def healpixCell(a: String, d: String, ns: Long): Double = {

    try {
      var alpha = a.toDouble
      var delta = d.toDouble
      var phi: Double = (90 - delta).toRadians
      var theta: Double = alpha.toRadians
      var ptg: Pointing = new Pointing(phi, theta)
      var b = new HealpixBase(ns, Scheme.NESTED);
      var ipix = b.ang2pix(ptg)
      return ipix
    } catch {
      case e: Exception => return (-1.0)
    }
  }

  def ExtractHealpix(a: String): Int = {

    var l = a.toLong
    var b = java.lang.Long.toBinaryString(l)
    while (b.length() < 64) {
      b = "0" + b
    }

    var str = b.substring(0, 31)
    val f = java.lang.Integer.parseInt(str, 2)
    return f

  }
}

