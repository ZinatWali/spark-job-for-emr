package org.simple.makebelieve

import org.apache.spark.sql.{SaveMode, SparkSession}

object SimpleJob {
  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MyApp")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("2014GSSNDI.csv")


    val refined = df
      .groupBy("year", "relig")
      .count()
      .cache()

    refined
      .write
      .mode(SaveMode.Overwrite)
      .csv("religion-by-year")

    spark.stop()
  }
}
