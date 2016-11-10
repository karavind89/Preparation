/**
  * Created by aravikri on 10/25/2016.
  */

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner


class JSON(sc: SparkContext){

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits

  case class SubClass(id:String, size:Int,useless:String)
  case class MotherClass(subClass: Array[SubClass])
  case class Record(id: String, size: Int)


  def Extract (input_location: String) = {

    //val df = sqlContext.read.json(input_location)
    //println(df.count())
    //println(df.printSchema())
    //println(df.select("acquiring_company"))
    //df.registerTempTable("temp")

    //val acquisition = sqlContext.sql("select acquisition from temp")
    //df.select("acquisition").show()
    //df.select("number_of_employees").show(1)
    //acquisition.collect().foreach(println)

    //Result : [[11,1,2010,[Progress Software,progress-software],49000000,USD,Progress Software buys Savvion for $49M,http://www.masshightech.com/stories/2010/01/11/daily3-Progress-Software-buys-Savvion-for-49M.html,null]]

    //Looping through JSON - Using Spark SQL
    //val acquisition_loop = sqlContext.sql("select acquisition.acquiring_company from temp")
    //acquisition_loop.collect().foreach(println)

    //Result : [[Emrise Corporation,emrise-corporation]]

    //Looping through JSON
    //df.filter(df("acquisition.price_amount") > 49000000 ).select("acquisition.acquiring_company.name","acquisition.acquired_year").foreach(println)

    //Grouping data
    //df.groupBy(df("acquisition.acquired_year")).count().show()

    //Sorting descending order
    //df.groupBy(df("acquisition.acquired_year")).count().sort(desc("count")).show(10)

    /*-- competitions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- competitor: struct (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- permalink: string (nullable = true)*/
    //val df_competitions = df.select("competitions.competitor.name","competitions.competitor.permalink")

    /*case class competitions (name: Array[String], permalink: Array[String])
    val temp = df_competitions.map{case Row( a: Array[String],b: Array[String]) => competitions( name = a, permalink = b) }
    print(temp) */


    /*
    //Transform JSON into DataFrame
    val json_obj = sc.parallelize(Array("""{"tweet": "hey man", "name": "Alan"}""", """{"tweet": "what's up", "name": "Bertha"}"""))
    val tweetsDF = sqlContext.read.json(json_obj)
    tweetsDF.show()
    */







  }

}

object Extract {

  def main(args: Array[String]): Unit ={

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("SparkAnalysis").setMaster("local")
  val sc = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  System.setProperty("hadoop.home.dir", "C:\\winutil\\")

  val input_location = "C:\\Users\\aravikri\\Downloads\\companies\\companies.json"

  val job = new JSON(sc)
  job.Extract(input_location)

  }
}

//References:
//https://spark.apache.org/docs/1.6.1/sql-programming-guide.html
//https://www.tutorialspoint.com/json/json_schema.htm
//https://www.supergloo.com/fieldnotes/spark-sql-json-examples/