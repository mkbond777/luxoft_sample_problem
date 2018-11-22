package com.luxoft

import java.io.File

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Sample extends App {

  override def main(args: Array[String]): Unit = {

    val sourceLoc = args(0)
    val spark = SparkSession
      .builder()
      .appName("loxoft")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val inputDf = spark.read.format("csv")
      .option("header", "true").load(sourceLoc)

    println("Num of processed files: " + Option(new File(sourceLoc).list.length).getOrElse(0))

    println("Num of processed measurements: " + inputDf.count())

    val failedMeasures = inputDf.filter(isnan(col("humidity")))
    println("Num of failed measurements: " + failedMeasures.count())

    agg(inputDf).show

  }

  def agg(df : DataFrame) : DataFrame = {

    val collectListDf = df.groupBy("sensor-id")
      .agg(collect_list("humidity").alias("col_list"))


    val customAgg = udf((groupedData: Seq[String], aggType: String) => {
      val noNaNList = groupedData.filter(!_.equals("NaN")).map(_.toInt)
      if (noNaNList.isEmpty)
        null
      else {
        aggType match {
          case "AVG" => (noNaNList.sum / noNaNList.length).toString
          case "MIN" => noNaNList.min.toString
          case "MAX" => noNaNList.max.toString
          case _ => null
        }
      }
    })

    val aggDf = collectListDf
      .withColumn("min", customAgg(col("col_list"), lit("MIN")))
      .withColumn("avg", customAgg(col("col_list"), lit("AVG")))
      .withColumn("max", customAgg(col("col_list"), lit("MAX"))).drop("col_list")

    aggDf.orderBy(col("avg").desc_nulls_last).na.fill("NaN")

  }

}
