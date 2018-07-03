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
import adql.query.constraint.{Comparison, In}
import org.apache.spark.sql.functions._
import adql.parser.ParseException
import adql.query.ADQLQuery
import adql.translator.TranslationException
import fr.uvsq.adam.astroide.{AstroideSession, AstroideUDF}
import fr.uvsq.adam.astroide.optimizer._
import fr.uvsq.adam.astroide.util.{Arguments, DirCheck}
import healpix.essentials.{HealpixProc, Pointing}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import fr.uvsq.adam.astroide.queries.optimized.KNNJoinCells
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source
import Console.{BLUE, GREEN, RED}

object AstroideQueries extends AstroideSession {


  val usage =

    """
      |Usage: AstroidQueries -fs hdfs://... infile infile2 healpixlevel queryfile action outfile x y x2 y2
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {

      case Nil ⇒ map
      case "-fs" :: hdfs :: tail =>
        parseArguments(map ++ Map('hdfs -> hdfs), tail)
      case infile :: infile2 :: healpixlevel :: queryfile :: action :: x :: y :: x2 :: y2 :: Nil ⇒
        map ++ Map('infile -> infile) ++ Map('infile2 -> infile2) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('queryfile -> queryfile) ++ Map('action -> action) ++ Map('outfile -> None) ++ Map('x -> x.toInt) ++ Map('y -> y.toInt) ++ Map('x2 -> x2.toInt) ++  Map('y2 -> y2.toInt)
      case infile :: infile2 :: healpixlevel :: queryfile :: action :: outfile :: x :: y :: x2 :: y2 :: Nil ⇒
        map ++ Map('infile -> infile) ++ Map('infile2 -> infile2) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('queryfile -> queryfile) ++ Map('action -> action) ++ Map('outfile -> outfile) ++ Map('x -> x.toInt) ++ Map('y -> y.toInt) ++ Map('x2 -> x2.toInt) ++  Map('y2 -> y2.toInt)
      case infile :: infile2 :: healpixlevel :: queryfile :: x :: y :: x2 :: y2 :: Nil ⇒
        map ++ Map('infile -> infile) ++ Map('infile2 -> infile2) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('queryfile -> queryfile) ++ Map('action -> None) ++ Map('outfile -> None) ++ Map('x -> x.toInt) ++ Map('y -> y.toInt) ++ Map('x2 -> x2.toInt) ++  Map('y2 -> y2.toInt)
      case option :: tail ⇒
        println(usage)
        throw new IllegalArgumentException(s"${RED}Unknown argument $option");
    }
  }


  def checkFile(file: String, hdfs: String) = {
    if (!DirCheck.dirExists(file, hdfs)) {
      throw new IOException(s"${RED}Input file " + file + " does not exist in HDFS"+Console.RESET)
    }
    else if (FilenameUtils.getExtension(file) != "parquet")
      throw new Exception(s"${RED}Input file " + file + " should be partitioned in parquet format"+Console.RESET)

    else if (!DirCheck.dirParquet(file, hdfs)) {
      throw new Exception(s"${RED}Input file " + file + " should be partitioned in parquet format\n Please use: " + BuildHealpixPartitioner.usage+Console.RESET)
    }
  }

  def checkOrder(order: Int) = {
    if (order < 0 || order > 29) {
      throw new Exception(s"${RED}HEALPix order should be in range [0,29]"+Console.RESET)
    }
  }

  def checkAction(action:String)= {
    val ListAction = List("count", "show", "save")
    if (!ListAction.contains(action))
      throw new Exception(s"${RED}Action should be listed in "+ListAction+Console.RESET)
  }

  def checkOutput(action:String, output:String)= {
    if (action=="save" && output == "None")
      throw new Exception(s"${RED}Please specify an output file to save result"+Console.RESET)
  }

  def PrintResult(action:String, result:DataFrame, output: String)=action match {

    case ("count") => println(s"${BLUE}Number of output result is: " +result.rdd.count()+Console.RESET)
    case ("show")  => result.show(20)
    case ("save") => {
      //val resultFile = astroideVariables.getFile1.substring(0,astroideVariables.getFile1.lastIndexOf("/")+1)
      result.write.mode(SaveMode.Overwrite).option("header",true).format("csv").save(output)
      //for duplicated column, add an alias
      println(s"${BLUE}Query result is saved on HDFS, please check directory "+ output+Console.RESET)
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

    val x = configuration('x).asInstanceOf[Int]
    val y = configuration('y).asInstanceOf[Int]
    val x2 = configuration('x2).asInstanceOf[Int]
    val y2 = configuration('y2).asInstanceOf[Int]


    val action = configuration('action).toString
    val output = configuration('outfile).toString

    checkOutput(action,output)

    checkFile(astroideVariables.getFile1, astroideVariables.getHDFS())
    checkOrder(astroideVariables.getOrder())

    import astroideSession.implicits._

    val inputData = astroideSession.read.parquet(astroideVariables.getFile1()).filter($"ipix".between(x,y))

    val testQuery = Source.fromFile(astroideVariables.getQueryFile).getLines.mkString

    AstroideUDF.RegisterUDFWithRules(astroideSession)

    try {

      val healpixClass =new Pointing(0, 0)

      val parser = new ADQLParser()
      val query = parser.parseQuery(testQuery)
      println(s"${GREEN}Correct ADQL Syntax"+Console.RESET)

      //astroideVariables.setRadius(query.getWhere.asInstanceOf[Comparison])

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

      if (ntables==1) {
        inputData.createOrReplaceTempView(it.next().toADQL)}
      else if (ntables==2) {
        if (njoin==1) {
          inputData.createOrReplaceTempView(it.next().toADQL)

          checkFile(astroideVariables.getFile2(), astroideVariables.getHDFS())

          val inputData2 = astroideSession.read.parquet(astroideVariables.getFile2()).filter($"ipix".between(x2,y2))

          inputData2.createOrReplaceTempView(it.next().toADQL)
        }
        else {
          it.next()
          inputData.createOrReplaceTempView(it.next().toADQL)
        }
      }

      else if (ntables==3) {
	      it.next()

	      inputData.createOrReplaceTempView(it.next().toADQL)
        checkFile(astroideVariables.getFile2(), astroideVariables.getHDFS())

	      val inputData2 = astroideSession.read.parquet(astroideVariables.getFile2()).filter($"ipix".between(x2,y2))

	      inputData2.createOrReplaceTempView(it.next().toADQL)
	  }

      else if (ntables==4) {


        println(inputData.count())

        def udfToHealpix = udf((alpha: Double, delta: Double) => {

          val theta = math.Pi / 2 - delta.toRadians
          val phi = alpha.toRadians

          HealpixProc.ang2pixNest(10, new Pointing(theta, phi))
        })

        val inputData2 = astroideSession.read.parquet(astroideVariables.getFile2()).withColumn("ipix10",udfToHealpix($"ra",$"dec")).filter($"ipix".between(x2,y2))
        val inputData1= inputData.withColumn("ipix10",udfToHealpix($"ra",$"dec"))

        checkFile(astroideVariables.getFile2(), astroideVariables.getHDFS())

        println(inputData2.count())


        val id2 = query.getWhere.adqlIterator().next().asInstanceOf[In].getOperand.toADQL

        astroideVariables.setid2(id2)

        val id1 = query.getSelect.get(0).toADQL

        astroideVariables.setid1(id1)

        val limit = query.getWhere.adqlIterator().next().asInstanceOf[In].getSubQuery.getSelect.getLimit

        astroideVariables.setLimit(limit)

        val hist = inputData2.select(id2,"ipix10").groupBy("ipix10")
        val histCount = hist.count()
        var histS = histCount.sort(desc("count"))


        val histSMap = histS.rdd.map(row => (row(0).asInstanceOf[Long], row(1).asInstanceOf[Long])).collectAsMap()

        //val histAllMap = KNNJoinCells.CreateHistogramAll(inputData1, inputData2, histS)


        val histR = inputData1.select("ipix10").except(inputData2.select("ipix10")).withColumn("count",lit(0L))

        val histogram = histR.union(histS)


        val histogramMap = histogram.rdd.map(row => (row(0).asInstanceOf[Long], row(1).asInstanceOf[Long])).collectAsMap()

        val sc: SparkContext = astroideSession.sparkContext
        sc.setLogLevel("ERROR")

        val br = sc.broadcast(histogramMap)

        val brS = sc.broadcast(histSMap)

        val NeighboursToJoin = udf((a: Long) => (KNNJoinCells.addNeighbours(a,br,brS,astroideVariables.getLimit(),astroideVariables.getOrder())+a).toList)

        var explodedData_1 = inputData1.withColumn("neighbours",explode(NeighboursToJoin($"ipix10")))

        explodedData_1.createOrReplaceTempView(it.next().toADQL)

        //inputData.createOrReplaceTempView(it.next().toADQL)

        inputData2.createOrReplaceTempView(it.next().toADQL)


        //astroideVariables.setHistogramS(histSMap)
        //astroideVariables.setHistogram(histAllMap)

        //astroideVariables.getHistogram().foreach(println)


        //KNNJoinCells.broadcastHistogram(inputData,inputData2,id2,spark)

	  }

      else throw new Exception(s"${RED}This case will be supported in future versions"+Console.RESET)

      val translatedQuery = MainTranslator.translateWithRules(query)
      println(s"${BLUE}== Translated Query ==\n"+translatedQuery+Console.RESET)

      val result = astroideSession.sql(translatedQuery)

      result.explain()

      val start = System.currentTimeMillis()

      PrintResult(action,result,output)

      val timeTotal = System.currentTimeMillis() - start

      println(s"${GREEN}Total query time " + timeTotal / 1000 + " sec "+Console.RESET)


    } catch {

      case a:ClassNotFoundException => println(s"${RED}Please defind a valid class path to HEALPix library " + a.getMessage+Console.RESET)
      case e: ParseException ⇒ println(s"${RED}ADQL syntax incorrect between " + e.getPosition() + ":" + e.getMessage()+Console.RESET)
      case f: TranslationException ⇒ println(s"${RED}ADQL Translation error " + f.getMessage+Console.RESET)
      case j: ClassCastException => println(s"${RED}Error occurred while executing the query " + j.printStackTrace()+Console.RESET)
      case p: Exception ⇒ println(s"${RED}Error occurred while executing the query" + p.printStackTrace()+Console.RESET)
    }

    astroideSession.stop()
  }

}

