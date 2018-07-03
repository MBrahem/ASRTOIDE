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
package fr.uvsq.adam.astroide.executor

import java.io.IOException

import adql.parser.ADQLParser
import adql.parser.ParseException
import adql.translator.TranslationException
import fr.uvsq.adam.astroide.{AstroideSession, AstroideUDF}
import fr.uvsq.adam.astroide.optimizer._
import fr.uvsq.adam.astroide.util.{Arguments, DirCheck}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import adql.query.constraint.In

import scala.io.Source

object TestQueries extends AstroideSession {


    val usage =

	"""
	  |Usage: AstroidQueries [-fs hdfs://...] infile infile2 healpixlevel queryfile action
	""".stripMargin

    type OptionMap = Map[Symbol, Any]

    def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
	arguments match {

	    case Nil ⇒ map
	    case "-fs" :: hdfs :: tail =>
		parseArguments(map ++ Map('hdfs -> hdfs), tail)
	    case infile :: infile2 :: healpixlevel :: queryfile :: action :: Nil ⇒
		map ++ Map('infile -> infile) ++ Map('infile2 -> infile2) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('queryfile -> queryfile) ++ Map('action -> action)
	    case infile :: infile2 :: healpixlevel :: queryfile :: Nil ⇒
		map ++ Map('infile -> infile) ++ Map('infile2 -> infile2) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('queryfile -> queryfile) ++ Map('action -> None)
	    case option :: tail ⇒
		println(usage)
		throw new IllegalArgumentException(s"Unknown argument $option");
	}
    }

    def checkFile(file: String, hdfs: String) = {
	if (!DirCheck.dirExists(file, hdfs)) {
	    throw new IOException("Input file " + file + " does not exist in HDFS")
	}
	else if (FilenameUtils.getExtension(file) != "parquet")
	    throw new Exception("Input file " + file + " should be partitioned in parquet format")

	else if (!DirCheck.dirParquet(file, hdfs)) {
	    throw new Exception("Input file " + file + " should be partitioned in parquet format\n Please use: " + BuildHealpixPartitioner.usage)
	}
    }

    def checkOrder(order: Int) = {
	if (order < 0 || order > 29) {
	    throw new Exception("HEALPix order should be in range [0,29]")
	}
    }

    def checkAction(action: String) = {
	val ListAction = List("count", "show", "save")
	if (!ListAction.contains(action))
	    throw new Exception("Action should be listed in " + ListAction)
    }

    def PrintResult(action: String, result: DataFrame) = action match {

	case ("count") => println(result.rdd.count())
	case ("show") => result.show(20)
	case ("save") => {
	    val resultFile = astroideVariables.getFile1.substring(0, astroideVariables.getFile1.lastIndexOf("/") + 1)
	    result.write.mode(SaveMode.Overwrite).format("csv").save(resultFile + "queryResult")
	    //for duplicated column, add an alias
	    println("Query result is saved in HDFS, please check directory " + resultFile + "queryResult")
	}
	case ("None") =>
    }

    var astroideVariables = new Arguments()

    def main(args: Array[String]) {

	if (args.length == 0) {
	    println(usage)
	}

	val configuration = parseArguments(Map(), args.toList)
	println(configuration)

	astroideVariables.setFile1(configuration('infile).toString)
	astroideVariables.setFile2(configuration('infile2).toString)
	astroideVariables.setOrder(configuration('healpixlevel).asInstanceOf[Int])
	astroideVariables.setQueryFile(configuration('queryfile).toString)
	astroideVariables.setHDFS(configuration('hdfs).toString)
	val action = configuration('action).toString

	checkFile(astroideVariables.getFile1, astroideVariables.getHDFS())
	checkOrder(astroideVariables.getOrder())

	val inputData = astroideSession.read.parquet(astroideVariables.getFile1())
	//import astroideSession.implicits._

	//val inputData = Seq((1, 10, 0.011), (1, 20, 0.012), (1, 30, 0.013),(1,40, 0.014), (1,50, 0.015),(1,60, 0.016), (2, 10, 0.011), (2, 20, 0.012), (2, 30, 0.013),(2,40, 0.014), (2,50, 0.015),(2,60, 0.016)
	//, (3, 10, 0.011), (3, 20, 0.012), (3, 30, 0.013),(3,40, 0.014), (3,50, 0.015),(3,60, 0.016), (4, 10, 0.011), (4, 20, 0.012), (4, 30, 0.013),(4,40, 0.014), (4,50, 0.015),(4,60, 0.016)).toDF("idr", "ids", "dist")


	val testQuery = Source.fromFile(astroideVariables.getQueryFile).getLines.mkString

	AstroideUDF.RegisterUDFWithRules(astroideSession)

	try {

	    val parser = new ADQLParser()
	    val query = parser.parseQuery(testQuery)
	    println("Correct ADQL Syntax")


	    val translatedQuery = MainTranslator.translateWithRules(query)
	    println("== Translated Query ==\n" + translatedQuery)

	    //inputData.createOrReplaceTempView("x_match")
	    inputData.createOrReplaceTempView("R")

	    checkFile(astroideVariables.getFile2(), astroideVariables.getHDFS())

	    val inputData2 = astroideSession.read.parquet(astroideVariables.getFile2())

	    inputData2.createOrReplaceTempView("S")

			val from = new SearchFrom()
			from.search(query)

			val ntables = from.getNbMatch
			val it = from.iterator()


			val join = new SearchJoin()
			join.search(query)

			val kNNjoin = new SearchKNNJoin()
			kNNjoin.search(query)

			val ncross = kNNjoin.getNbMatch

			val njoin = join.getNbMatch

			val constraint = new SearchConstraints()
			constraint.search(query)


			println(constraint.getNbMatch)

			println(ntables)
		 	println(ncross)
			println(njoin)

			val test = query.getWhere.adqlIterator().next().asInstanceOf[In]

		 println(test.getOperand.toADQL)


			//val result = astroideSession.sql("Select * from (SELECT *, rank() over (Partition by source_id order by dist) As rank from (SELECT * , SphericalDistance(R.ra, R.dec, S.ra, S.dec) AS dist FROM R CROSS JOIN S) x_match ) t where rank <= 5 ")

			val result = astroideSession.sql("SELECT re.souce_id, se.TYC1 FROM R re, S se WHERE se.TYC1 IN (select x.TYC1 FROM (SELECT * , SphericalDistance(re.ra, re.dec, si.ra, si.dec) AS dist FROM S si ) AS x ORDER by x.dist limit 10)")

	    //val result = astroideSession.sql("Select * from (SELECT *, rank() over (Partition by source_id order by dist) As rank from (SELECT * , SphericalDistance(R.ra, R.dec, S.ra, S.dec) AS dist FROM R JOIN S ON (SphericalDistance(R.ra,R.dec,S.ra,S.dec) <0.0027)) x_match ) t where rank <= 10 ")

	    //result.explain(true)

	    //PrintResult(action, result)

	} catch {
	    case e: ParseException ⇒ println("ADQL syntax incorrect between " + e.getPosition() + ":" + e.getMessage())
	    case f: TranslationException ⇒ println("ADQL Translation error " + f.getMessage)
	    case j: ClassCastException => println("Error occurred while executing the query " + j.printStackTrace())
	    case p: Exception ⇒ println("Error occurred while executing the query" + p.printStackTrace())
	}

	astroideSession.stop()
    }

}


