package com.luxoft

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, isnan}
import org.scalatest.FunSuite

/**
  * Created by DT2(M.Kumar) on 11/22/2018.
  */
class SampleTest extends FunSuite {


  val spark: SparkSession = SparkSession
    .builder()
    .appName("loxoft")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")

  test("DataFrame without NAN") {

    import spark.implicits._

    val df = Seq(("s1",10),("s2",88),("s2",80),("s2",78),("s1",98),("s3",100)).toDF("sensor-id","humidity")

    Sample.agg(df).show
  }

  test("DataFrames containing one NaN and one integer value for each unique id") {

    import spark.implicits._

    val df = Seq(("s1","10"),("s2","88"),("s2","80"),("s2","NaN"),("s1","NaN"),("s3","100"),("s3","NaN")
    ).toDF("sensor-id","humidity")

    val failedMeasures = df.filter(isnan(col("humidity")))

    println("Num of failed measurements: " + failedMeasures.count())

    Sample.agg(df).show
  }

  test("DataFrames containing one unique Id with only NaN ") {


    import spark.implicits._

    val df = Seq(("s1","10"),("s2","88"),("s2","80"),("s2","NaN"),("s1","NaN"),("s3","NaN")
    ).toDF("sensor-id","humidity")

    Sample.agg(df).show
  }

}
