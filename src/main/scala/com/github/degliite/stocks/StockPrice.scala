package com.github.degliite.stocks

import org.apache.spark.sql.SparkSession
import com.github.mrpowers.spark.daria.sql.DariaWriters
import org.apache.spark.sql.functions.{avg, col, expr}

object StockPrice extends App{

  //Loading csv file
  val session = SparkSession.builder().appName("StockPrices").master("local").getOrCreate()
  val fPath = "./src/main/resources/stock_prices.csv"

  val df = session.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fPath)

  //Calculating average return based on "close" price
  val dailyReturn = df
    .groupBy(col("date"))
    .agg(avg("close").alias("average_return"))
    .withColumn("average_return_rounded", expr("ROUND(average_return, 2)"))
    .select("date","average_return_rounded")
    .orderBy(col("date"))

  // Saving avg daily returns in .CSV in a single report
  DariaWriters.writeSingleFile(
    df = dailyReturn,
    format = "csv",
    sc = session.sparkContext,
    tmpFolder = "./src/main/resources/tmp1", //the place where the files are stored before deleting
    filename = "./src/main/resources/dailyReturnCSV.csv"
  )

// Saving avg daily returns in .Parquet in a single report
  DariaWriters.writeSingleFile(
    df = dailyReturn,
    format = "parquet",
    sc = session.sparkContext,
    tmpFolder = "./src/main/resources/tmp2", //the place where the files are stored before deleting
    filename = "./src/main/resources/dailyReturnPARQUET.parquet"
  )

}
