package org.simple.makebelieve

import org.apache.spark.sql.{SaveMode, SparkSession}


object SimpleJob {
  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("MyApp")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("s3n://data-bucket-phase-1/2014GSSNDI.csv")

    val refined = df
      .groupBy("year", "relig")
      .count()
      .cache()

    refined
      .write
      .mode(SaveMode.Overwrite)
      .csv("s3n://data-bucket-phase-1/religion-by-year")

    spark.stop()
  }
}
