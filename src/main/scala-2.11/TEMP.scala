import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by aravikri on 11/7/2016.
  */
object TEMP {



  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("SparkAnalysis").setMaster("local")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._



  }

}
