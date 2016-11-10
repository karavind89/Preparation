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
import org.apache.spark.sql.functions.{lit, col, coalesce}
import org.apache.spark.sql.Column


class Transform (sc: SparkContext){

  /*
  //Create new column with function in Spark Dataframe

    import org.apache.spark.sql.functions._
    val myDF = sqlContext.parquetFile("hdfs:/to/my/file.parquet")
    val coder: (Int => String) = (arg: Int) => {if (arg < 100) "little" else "big"}
    val sqlfunc = udf(coder)
    myDF.withColumn("Code", sqlfunc(col("Amt")))
    */

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Must add this while converting to dataframe
    import sqlContext.implicits._

 /*
  //Derive multiple columns from a single column in a Spark DataFrame - Simple Method

    val df_1 = sc.parallelize(List(("Mike,1986,Toronto", 30), ("Andre,1980,Ottawa", 36), ("jill,1989,London", 27))).toDF("infoComb", "age")

    df_1.select(expr("(split(infoComb, ','))[0]").cast("string").as("name"), expr("(split(infoComb, ','))[1]").cast("integer")
    .as("yearOfBorn"), expr("(split(infoComb, ','))[2]").cast("string").as("city"), $"age").show()

  */

  /*
  //Derive multiple columns from a single column in a Spark DataFrame - Method 2

    case class Foobar(foo: Double, bar: Double) //declare the case class outside the method, it will work

    val df_2 = sc.parallelize(Seq((1L, 3.0, "a"), (2L, -1.0, "b"), (3L, 0.0, "c"))).toDF("x", "y", "z")
    val foobarUdf = udf((x: Long, y: Double, z: String) =>  Foobar(x * y, z.head.toInt * y))
    val df_3 = df_2.withColumn("foobar", foobarUdf($"x", $"y", $"z"))

    // +---+----+---+------------+
    // |  x|   y|  z|      foobar|
    // +---+----+---+------------+
    // |  1| 3.0|  a| [3.0,291.0]|
    // |  2|-1.0|  b|[-2.0,-98.0]|
    // |  3| 0.0|  c|   [0.0,0.0]|
    // +---+----+---+------------+
    //Return a column of complex type. The most general solution is a StructType but you can consider ArrayType or MapType as well.
  */

  /*
  //Derive multiple columns from a single column in a Spark DataFrame - Switch to RDD, reshape and rebuild DF

    val df_2 = sc.parallelize(Seq((1L, 3.0, "a"), (2L, -1.0, "b"), (3L, 0.0, "c"))).toDF("x", "y", "z")
    def foobarFunc(x: Long, y: Double, z: String): Seq[Any] =  Seq(x * y, z.head.toInt * y)
    val schema = StructType(df_2.schema.fields ++  Array(StructField("foo", DoubleType), StructField("bar", DoubleType)))
    val rows = df_2.rdd.map(r => Row.fromSeq( r.toSeq ++  foobarFunc(r.getAs[Long]("x"), r.getAs[Double]("y"), r.getAs[String]("z"))))
    val df2 = sqlContext.createDataFrame(rows, schema)
    df2.show()

  // +---+----+---+----+-----+
  // |  x|   y|  z| foo|  bar|
  // +---+----+---+----+-----+
  // |  1| 3.0|  a| 3.0|291.0|
  // |  2|-1.0|  b|-2.0|-98.0|
  // |  3| 0.0|  c| 0.0|  0.0|
  // +---+----+---+----+-----+

  Reference : http://stackoverflow.com/questions/32196207/derive-multiple-columns-from-a-single-column-in-a-spark-dataframe?noredirect=1&lq=1
  */

  /*
  //Partitioning Spark Dataframe - Pre-partition input data before you create a DataFrame

   val schema = StructType(Seq(
      StructField("x", StringType, false),
      StructField("y", LongType, false),
      StructField("z", DoubleType, false)
    ))

   val rdd = sc.parallelize(Seq(
      Row("foo", 1L, 0.5), Row("bar", 0L, 0.0), Row("??", -1L, 2.0),
      Row("foo", -1L, 0.0), Row("??", 3L, 0.6), Row("bar", -3L, 0.99)
    ))

   val partitioner = new HashPartitioner(5)

   val partitioned = rdd.map(r => (r.getString(0), r))
    .partitionBy(partitioner)
    .values

   val df = sqlContext.createDataFrame(partitioned, schema)
   df.show()

  //Repartition existing DataFrame

    sqlContext.createDataFrame(
      df.rdd.map(r => (r.getInt(1), r)).partitionBy(partitioner).values,
      df.schema
    )

  //http://stackoverflow.com/questions/30995699/how-to-define-partitioning-of-a-spark-dataframe?rq=1
  */

  /*
  //How to select a subset of fields from an array column in Spark?

  val df = sqlContext.createDataFrame(List(
    MotherClass(Array(
      SubClass("1",1,"thisIsUseless"),
      SubClass("2",2,"thisIsUseless"),
      SubClass("3",3,"thisIsUseless")
    )),
    MotherClass(Array(
      SubClass("4",4,"thisIsUseless"),
      SubClass("5",5,"thisIsUseless")
    ))
  ))

  val dropUseless = udf((xs: Seq[Row]) =>  xs.map{
    case Row(id: String, size: Int, _) => Record(id, size)
  })

  df.select(dropUseless($"subClass")).show()
  */

  /**
    * Scenario : I have a DataFrame created by running sqlContext.read of a Parquet file.The DataFrame consists of 300 M rows.
    * I need to use these rows as input to another function, but I want to do it in smaller batches to prevent OOM error.

    var count:long = df.count()
    val limit:Int = 50
    while(count > 0){
    df1 = df.limit(limit)
    df1.show();         //will print 50, next 50, etc rows
    df = df.except(df1)
    count = count - limit
      }
    **/

  //How to aggregate both numerical and nominal columns
  //df.groupyBy("id").agg(sum("amount"), collect_list("donor")) OR df.groupyBy("id").agg(sum("amount"), collect_set("donor"))

  // sample data: not the same order, not all records have all columns:
  val inputDF: DataFrame = sc.parallelize(Seq(
    ("EEUU", "2016-10-03", "T_D: QQWE\nT_NAME: name_1\nT_IN: ind_1\nT_C: c1ws12"),
    ("EEUU", "2016-10-03", "T_D: QQAA\nT_IN: ind_2\nT_NAME: name_2")
  )).toDF("country", "date_data", "text")

  //inputDF.show(10,false)
  //inputDF.collect.foreach(println)

  //Extract a column based on another column

  // Dummy data
  val df = sc.parallelize(Seq(
    (49.5, "EUR", 99, 79, 69), (100.0, "GBP", 80, 120, 50)
  )).toDF("paid", "currency", "EUR", "USD", "GBP")

  // A list of available currencies
  val currencies: List[String] = List("EUR", "USD", "GBP")

  // Select listed value
  val listedPrice: Column = coalesce(
    currencies.map(c => when($"currency" === c, col(c)).otherwise(lit(null))): _*)






}

/**
  * Created by aravikri on 11/8/2016.
  */
object Spark_Bible {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SparkAnalysis").setMaster("local")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    System.setProperty("hadoop.home.dir", "C:\\winutil\\")

    new Transform(sc)
  }
}
